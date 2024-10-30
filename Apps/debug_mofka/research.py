import os
import time
import numpy as np
from dask.distributed import LocalCluster
from dask.distributed import Client
from dask.array.lib.stride_tricks import sliding_window_view
from dask.distributed import wait as Wait
from dask.delayed import delayed
import dask.array as da
import dask.dataframe as dd
import dask
import uuid
import pandas as pd
import math
import pynvml
import csv
import time
import psutil
import threading

global stop_condition



def monitor_worker_thread(worker_id):
    pynvml.nvmlInit()
    device_index = worker_id % 4
    handle = pynvml.nvmlDeviceGetHandleByIndex(device_index)
    
    # Open the CSV file in append mode ('a'), ensuring data is added in case of shutdown or restart
    with open(f"worker_{worker_id}_stats.csv", mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # If the file is empty, write the header
        file.seek(0, 2)  # Go to the end of the file to check if it's empty
        if file.tell() == 0:
            writer.writerow(['Timestamp', 'System_MEM','USED_SYSTEM_MEM', 'cpu_mem', 'cpu_util', 'r_count', 'r_bytes', 'w_count', 'w_byes', 'GPU_MEM', 'Free_GPU_MEM', 'GPU_USED_MEM', 'GPU_util', 'GPU_mem_util', 'GPU_temp', 'GPU_mW'])

        
        while True:
            system_memory = psutil.virtual_memory()
            
            cpu_mem = system_memory.total
            cpu_used_mem = system_memory.used
            current_process = psutil.Process()
            memory_info = current_process.memory_info()

            # RSS (Resident Set Size) is the actual memory the process is using in bytes
            rss_memory = memory_info.rss
            

            cpu_percent = current_process.cpu_percent(interval=1)  # interval=1 to get the average over 1 second
            
            
            io_counters = current_process.io_counters()
            read_count = io_counters.read_count
            read_bytes = io_counters.read_bytes
            write_count = io_counters.write_count
            write_bytes = io_counters.write_bytes


            # Get memory info
            memory_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            total_memory = memory_info.total    # Raw value in bytes
            free_memory = memory_info.free      # Raw value in bytes
            used_memory = memory_info.used      # Raw value in bytes
            
            # Get utilization rates (GPU and memory utilization)
            utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
            gpu_utilization = utilization.gpu        # GPU utilization in percentage
            memory_utilization = utilization.memory  # Memory utilization in percentage
            
            # Get temperature (in Celsius)
            temperature = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
            
            # Get power usage (in milliwatts, raw)
            power_usage = pynvml.nvmlDeviceGetPowerUsage(handle)  # Raw value in milliwatts
            
            # Get the current timestamp
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Write the stats to the CSV file
            writer.writerow([timestamp, cpu_mem, cpu_used_mem, rss_memory, cpu_percent, read_count, read_bytes, write_count, write_bytes, total_memory, free_memory, used_memory, gpu_utilization, 
                            memory_utilization, temperature, power_usage])
                            
            # Print the values (optional)
            # print(f"{timestamp} - Total: {total_memory} bytes, Free: {free_memory} bytes, Used: {used_memory} bytes, "
            #     # f"GPU Util: {gpu_utilization}%, Memory Util: {memory_utilization}%, "
            #     f"Temp: {temperature} C, Power: {power_usage} mW")
            
            # Flush the data to the file (to ensure data is saved after each iteration)
            file.flush()
            
            # Sleep for 1 second before getting the next reading
            time.sleep(1)

        

    # Shutdown NVML when done
    print("Exiting",flush=True)
    pynvml.nvmlShutdown()


def monitor_local_thread():
    # Open the CSV file in append mode ('a'), ensuring data is added in case of shutdown or restart
    with open(f"local_stats.csv", mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # If the file is empty, write the header
        file.seek(0, 2)  # Go to the end of the file to check if it's empty
        if file.tell() == 0:
            writer.writerow(['Timestamp', 'System_MEM','USED_SYSTEM_MEM', 'cpu_mem', 'cpu_util', 'r_count', 'r_bytes', 'w_count', 'w_byes'])

        global stop_condition
        while stop_condition:
            system_memory = psutil.virtual_memory()
            
            cpu_mem = system_memory.total
            cpu_used_mem = system_memory.used
            current_process = psutil.Process()
            memory_info = current_process.memory_info()

            # RSS (Resident Set Size) is the actual memory the process is using in bytes
            rss_memory = memory_info.rss
            

            cpu_percent = current_process.cpu_percent(interval=1)  # interval=1 to get the average over 1 second
            
            
            io_counters = current_process.io_counters()
            read_count = io_counters.read_count
            read_bytes = io_counters.read_bytes
            write_count = io_counters.write_count
            write_bytes = io_counters.write_bytes


      
            
            
            # Get the current timestamp
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Write the stats to the CSV file
            writer.writerow([timestamp, cpu_mem, cpu_used_mem, rss_memory, cpu_percent, read_count, read_bytes, write_count, write_bytes])
                            
            # # Print the values (optional)
            # print(f"{timestamp} - Total: {total_memory} bytes, Free: {free_memory} bytes, Used: {used_memory} bytes, "
            #     f"GPU Util: {gpu_utilization}%, Memory Util: {memory_utilization}%, "
            #     f"Temp: {temperature} C, Power: {power_usage} mW")
            
            # Flush the data to the file (to ensure data is saved after each iteration)
            file.flush()
            
            # Sleep for 1 second before getting the next reading
            time.sleep(1)

        

    # Shutdown NVML when done
    print("Exiting local",flush=True)
class DCRNNSupervisor:
    def __init__(self, adj_mx, **kwargs):
        self._kwargs = kwargs
        self._data_kwargs = kwargs.get('data')
        self._model_kwargs = kwargs.get('model')
        self._train_kwargs = kwargs.get('train')

        self.max_grad_norm = self._train_kwargs.get('max_grad_norm', 1.)

        # logging.
        # self._log_dir = self._get_log_dir(kwargs)
        # self._writer = SummaryWriter('runs/' + self._log_dir)

        # log_level = self._kwargs.get('log_level', 'INFO')
        # self._logger = utils.get_logger(self._log_dir, __name__, 'info.log', level=log_level)

        # data set
        # self._data = utils.load_dataset(**self._data_kwargs)
        # self.standard_scaler = self._data['scaler']

        self.num_nodes = int(self._model_kwargs.get('num_nodes', 1))
        self.input_dim = int(self._model_kwargs.get('input_dim', 1))
        self.seq_len = int(self._model_kwargs.get('seq_len'))  # for the encoder
        self.output_dim = int(self._model_kwargs.get('output_dim', 1))
        self.use_curriculum_learning = bool(
            self._model_kwargs.get('use_curriculum_learning', False))
        self.horizon = int(self._model_kwargs.get('horizon', 1))  # for the decoder

        # setup model
        # dcrnn_model = DCRNNModel(adj_mx, self._logger, **self._model_kwargs)
        # model = dcrnn_model.cuda() if torch.cuda.is_available() else dcrnn_model
        # self._logger.info("Model created")

        self._epoch_num = self._train_kwargs.get('epoch', 0)
        
        # if self._epoch_num > 0:
        #     self.load_model()
        
    
    @staticmethod
    def _get_log_dir(kwargs):
        log_dir = kwargs['train'].get('log_dir')
        if log_dir is None:
            batch_size = kwargs['data'].get('batch_size')
            learning_rate = kwargs['train'].get('base_lr')
            max_diffusion_step = kwargs['model'].get('max_diffusion_step')
            num_rnn_layers = kwargs['model'].get('num_rnn_layers')
            rnn_units = kwargs['model'].get('rnn_units')
            structure = '-'.join(
                ['%d' % rnn_units for _ in range(num_rnn_layers)])
            horizon = kwargs['model'].get('horizon')
            filter_type = kwargs['model'].get('filter_type')
            filter_type_abbr = 'L'
            if filter_type == 'random_walk':
                filter_type_abbr = 'R'
            elif filter_type == 'dual_random_walk':
                filter_type_abbr = 'DR'
            run_id = 'dcrnn_%s_%d_h_%d_%s_lr_%g_bs_%d_%s/' % (
                filter_type_abbr, max_diffusion_step, horizon,
                structure, learning_rate, batch_size,
                time.strftime('%m%d%H%M%S'))
            base_dir = kwargs.get('base_dir')
            log_dir = os.path.join(base_dir, run_id)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        return log_dir

    def train(self, **kwargs):
        kwargs.update(self._train_kwargs)

        return self._train(**kwargs)

    def train(self, **kwargs):
        kwargs.update(self._train_kwargs)

        return self._train(**kwargs)

    

    def _train(self, base_lr,
               steps, patience=50, epochs=100, lr_decay_ratio=0.1, log_every=1, save_model=1,
               test_every_n_epochs=10, epsilon=1e-8, **kwargs):
        
        def readPD():
            df = pd.read_hdf(kwargs['h5'], key=kwargs['h5key'])
            print(kwargs['h5'], flush=True)
            df = df.astype('float32')
            if 'bay' in kwargs['h5']:
                pass
            else:
                df.index.freq='5min'  # Manually assign the index frequency
            df.index.freq = df.index.inferred_freq
            return df
        # steps is used in learning rate - will see if need to use it?
        def get_numeric_part(filename):
            return int(filename.split("_")[1].split(".")[0])
        
        
        if self._train_kwargs['load_path'] == "auto":
            current_dir = os.getcwd()
            files = os.listdir(current_dir)
            rel_files = [f for f in files if ".pth" in f]

            if len(rel_files) > 0:
                sorted_filenames = sorted(rel_files, key=get_numeric_part)
                self._train_kwargs['load_path'] = sorted_filenames[-1]
            else:
                self._train_kwargs['load_path'] = None


        global stop_condition
        stop_condition = True
        # if self._train_kwargs['profile'] == True:
        #     thread = threading.Thread(target=monitor_local_thread)
        #     thread.start()


        start_time = time.time()
        if kwargs['mode'] == 'local':
            cluster = LocalCluster(n_workers=kwargs['npar'])
            client = Client(cluster)
        elif kwargs['mode'] == 'dist':
            client = Client(scheduler_file = f"cluster.info")
        else:
            print(f"{kwargs['mode']} is not a valid mode; Please enter mode as either 'local' or 'dist'")
            exit()
        
        x_train=None
        y_train=None
        x_val=None
        y_val=None
        mean=None
        std=None
        data_length=None
        data = None

        if self._train_kwargs['impl'].lower() == "dask":
            print("Dask implementation in use ", flush=True)
            # dask.config.set({"deloy.lost-worker-timeout": "100000s"}) 
            
        elif self._train_kwargs['impl'].lower() == "lazydask":
            print("Lazy-dask-batching implementation in use ", flush=True)
            # dask.config.set({"deloy.lost-worker-timeout": "100000s"}) 
            
        elif self._train_kwargs['impl'].lower() == "index":
            print("Index-batching implementation in use ", flush=True)
        
        elif self._train_kwargs['impl'].lower() == "dask-index":
            print("Dask-Index-batching implementation in use ", flush=True)
        
        else:
            print(f"{self._train_kwargs['impl'].lower()} is not a valid option; Please enter impl as either 'dask-index', 'index','dask', or 'lazyDask")
            exit()
        
        
        if self._train_kwargs['impl'].lower() == "dask-index":
            
            
            t1 = time.time()
                # exit()
                # 0 is the latest observed sample.
            dfs = delayed(readPD)()
            df = dd.from_delayed(dfs)
            min_val_loss = float('inf')
            

            num_samples, num_nodes = df.shape

            num_samples = num_samples.compute()
            
            x_offsets = np.sort(np.arange(-11, 1, 1))
            y_offsets = np.sort(np.arange(1, 13, 1))
            
            # print("\rStep 1a Starting: df.to_dask_array", flush=True)
            data1 =  df.to_dask_array(lengths=True)
            # print(data1.shape)
            data1 = da.expand_dims(data1, axis=-1)
            data1 = data1.rechunk("auto")



            # print("\rStep 1b Starting: Tiling", flush=True)
            # print("kwargs at top: ", kwargs, flush=True)
            data2 = da.tile((df.index.values.compute() - df.index.values.compute().astype("datetime64[D]")) / np.timedelta64(1, "D"), [1, num_nodes, 1]).transpose((2, 1, 0))
            data2 = data2.rechunk((data1.chunks))
            data = da.concatenate([data1, data2], axis=-1)

            x, y = [], []
            # t is the index of the last observation.
            min_t = abs(min(x_offsets))
            max_t = abs(num_samples - abs(max(y_offsets)))  # Exclusive
            x_i = np.arange(x_offsets[0] + max_t)
            data_length = x_offsets[0] + max_t

            concat = False
            # concat method
            bin_len = round(x_i.shape[0] * 0.7) + 11
            # print("bin len", bin_len)
            if concat:
                ascending = np.arange(1, 12)
                descending = np.arange(11, 0, -1)
                remaining_elements = bin_len - (len(ascending) + len(descending))
                print("remain: ", remaining_elements)
                repeat_12 = np.full(remaining_elements, 12)
                
                # Step 3: Concatenate the sequences
                bin = np.concatenate((ascending, repeat_12, descending))

            else: 
                # pre-alloc
                bin = np.empty(bin_len, dtype=int)
                
                # Step 2: Fill in the ascending part [1, 2, ..., 12]
                bin[:12] = np.arange(1, 13)
                # Step 3: Calculate how many times to repeat 12
                remaining_elements = bin_len - 24
                bin[12:13+remaining_elements] = 12
                
                # Step 4: Fill in the descending part [11, 10, ..., 1]
                bin[13+remaining_elements:] = np.arange(11, 0, -1)
            

            num_samples = x_i.shape[0]
            num_test = round(num_samples * 0.2)
            num_train = round(num_samples * 0.7)
            num_val = num_samples - num_test - num_train

            

            x_train = x_i[:num_train]
            x_val = x_i[num_train: num_train + num_val]
            # print(x_train.shape)
            cutoff = bin.shape[0]
            
            total_entries = x_train.shape[0] * 12 * num_nodes
        
            mean = (data[: cutoff,..., 0].sum(axis=1) * bin).sum() / total_entries
            std = da.sqrt(  ((da.square(data[: cutoff, ..., 0] - mean)).sum(axis=1) * bin ).sum() / total_entries )
            

            data[..., 0] = (data[..., 0] - mean) / std
            data = data.rechunk("auto")

            for i in [mean, std, x_train, x_val]:
                print(type(i))
            data, mean, std,  = client.persist([data, mean, std])
            
            
            Wait([data, mean, std])
            mean = mean.compute()
            std = std.compute()
            
            pre_end = time.time()
            print(f"Preprocessing complete in {pre_end - t1}; Training Starting")
            
            if os.path.exists("stats.txt"):
                with open("stats.txt", "a") as file:
                        file.write(f"pre_processing_time: {pre_end - t1}\n")
            else:
                with open("stats.txt", "a") as file:
                    file.write(f"pre_processing_time: {pre_end - t1}\n")

        if self._train_kwargs['impl'].lower() == 'lazydask' or self._train_kwargs['impl'].lower() == 'dask':
            print("Starting dask preprocessing", flush=True)
            t1 = time.time()
            
            dfs = delayed(readPD)()
            df = dd.from_delayed(dfs)
            min_val_loss = float('inf')
            

            num_samples, num_nodes = df.shape

            num_samples = num_samples.compute()
            
            x_offsets = np.sort(np.arange(-11, 1, 1))
            y_offsets = np.sort(np.arange(1, 13, 1))
            
            # print("\rStep 1a Starting: df.to_dask_array", flush=True)
            data1 =  df.to_dask_array(lengths=True)
            # print(data1.shape)
            data1 = da.expand_dims(data1, axis=-1)
            data1 = data1.rechunk("auto")



            # print("\rStep 1b Starting: Tiling", flush=True)
            # print("kwargs at top: ", kwargs, flush=True)
            data2 = da.tile((df.index.values.compute() - df.index.values.compute().astype("datetime64[D]")) / np.timedelta64(1, "D"), [1, num_nodes, 1]).transpose((2, 1, 0))
            data2 = data2.rechunk((data1.chunks))
            
            
            # print("\rStep 1c Starting: Tiling", end="\n", flush=True)
            memmap_array = da.concatenate([data1, data2], axis=-1)
            

            del df
            # print("\rStep 1a Done; Step 1b Starting", flush=True)






            del data1 
            del data2

            
            min_t = abs(min(x_offsets))
            max_t = abs(num_samples - abs(max(y_offsets)))  # Exclusive
            total = max_t - min_t

            window_size = 12
            original_shape = memmap_array.shape

            
            # Define the window shape
            window_shape = (window_size,) + original_shape[1:]  # (12, 207, 2)

            # Use sliding_window_view to create the sliding windows
            sliding_windows = sliding_window_view(memmap_array, window_shape).squeeze()
            # time.sleep(15)
            # print(sliding_windows.compute().shape)
            # print(sliding_windows.compute())
            
            x_array = sliding_windows[:total]
            y_array = sliding_windows[window_size:]
            del memmap_array
            del sliding_windows





            num_samples = x_array.shape[0]
            num_test = round(num_samples * 0.2)
            num_train = round(num_samples * 0.7)
            num_val = num_samples - num_test - num_train

            

            x_train = x_array[:num_train]
            y_train = y_array[:num_train]
            # ycl_train = y_array[:num_train]

            # x_train = x_train
            # y_train = y_train
            # ycl_train = ycl_train

            # wait([x_train,y_train, ycl_train])


            # print("Step 3: Computing Mean and Std-Dev", flush=True)
            mean = x_train[..., 0].mean()
            std = x_train[..., 0].std()
            

            # print("Step 4a: Standardizing Train x Dataset",end="",  flush=True)
            x_train[..., 0] = (x_train[..., 0] - mean) / std
            y_train[..., 0] = (y_train[..., 0] - mean) / std
            # x_train = x_train)
            
            
            # print("\rStep 4b: Standardizing Train ycl Dataset",  flush=True)
            # ycl_train[..., 0] = (ycl_train[..., 0] - mean) / std
            # ycl_train = ycl_train)
            


            x_val = x_array[num_train: num_train + num_val]
            y_val = y_array[num_train: num_train + num_val]



            # print("Step 5: Standardizing Validation Dataset")
            x_val[..., 0] = (x_val[..., 0] - mean) / std
            y_val[..., 0] = (y_val[..., 0] - mean) / std
            
            # x_val = x_val)
            # print("\rStep 1c: Concat, window, standardize" , flush=True)
            mean, std, x_train, y_train, x_val, y_val = client.persist([mean, std, x_train, y_train, x_val, y_val])
            
            
            Wait([mean, std, x_train, y_train, x_val, y_val])
            
            # time.sleep(30)
            mean = mean.compute()
            std = std.compute()

      


            pre_end = time.time()
            print("Mean: ", mean, flush=True)
            print(f"Preprocessing complete in {pre_end - t1}; Training Starting", flush=True)
            
            if os.path.exists("stats.txt"):
                with open("stats.txt", "a") as file:
                        file.write(f"pre_processing_time: {pre_end - t1}\n")
            else:
                with open("stats.txt", "a") as file:
                    file.write(f"pre_processing_time: {pre_end - t1}\n")

            
            del x_array
            del y_array
        
        time.sleep(30)
        client.shutdown()
        stop_condition = False