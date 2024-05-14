import sys
import os

from   distributed import Client
import time
import yaml
from datetime import datetime

import uuid
import datetime
import pickle
import json
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import urllib.request
from torch.utils.data import Dataset, DataLoader

from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist
from torch.utils.data.distributed import DistributedSampler
from dask_pytorch_ddp import dispatch, results
from dask.distributed import Client
from distributed.worker import logger



def validate(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file):

    # Validate mode
    if mode == "MPI":
        from dask_mpi import initialize
        initialize()
        client = Client()
    elif mode == "distributed":
        if scheduler_file:
            client= Client(scheduler_file = scheduler_file)
        else:
            raise ValueError("When distributed Mode is activated the the path to the scheduler-file must be specified, current value is %s: " % scheduler_file)
    elif mode == "LocalCluster" or mode is None:
        client = Client(processes=False)
    else:
        raise ValueError("Unknown launching mode %s" % mode)

    return client

def format_training_data(pet_names, device=None):
    def get_substrings(in_str):
        # add the stop character to the end of the name, then generate all the partial names
        in_str = in_str + "+"
        res = [in_str[0:j] for j in range(1, len(in_str) + 1)]
        return res

    pet_names_expanded = [get_substrings(name) for name in pet_names]
    pet_names_expanded = [item for sublist in pet_names_expanded for item in sublist]
    pet_names_characters = [list(name) for name in pet_names_expanded]
    pet_names_padded = [name[-(str_len + 1) :] for name in pet_names_characters]
    pet_names_padded = [
        list((str_len + 1 - len(characters)) * "*") + characters for characters in pet_names_padded
    ]
    pet_names_numeric = [[characters.index(char) for char in name] for name in pet_names_padded]

    # the final x and y data to use for training the model. Note that the x data needs to be one-hot encoded
    if device is None:
        y = torch.tensor([name[1:] for name in pet_names_numeric])
        x = torch.tensor([name[:-1] for name in pet_names_numeric])
    else:
        y = torch.tensor([name[1:] for name in pet_names_numeric], device=device)
        x = torch.tensor([name[:-1] for name in pet_names_numeric], device=device)
    x = torch.nn.functional.one_hot(x, num_classes=len(characters)).float()
    return x, y

class OurDataset(Dataset):
    def __init__(self, pet_names, device=None):
        self.x, self.y = format_training_data(pet_names, device)
        self.permute()

    def __getitem__(self, idx):
        idx = self.permutation[idx]
        return self.x[idx], self.y[idx]

    def __len__(self):
        return len(self.x)

    def permute(self):
        self.permutation = torch.randperm(len(self.x))

class Model(nn.Module):
    def __init__(self):
        super(Model, self).__init__()
        self.lstm_size = 128
        self.lstm = nn.LSTM(
            input_size=len(characters),
            hidden_size=self.lstm_size,
            num_layers=4,
            batch_first=True,
            dropout=0.1,
        )
        self.fc = nn.Linear(self.lstm_size, len(characters))

    def forward(self, x):
        output, state = self.lstm(x)
        logits = self.fc(output)
        return logits


def generate_name(model, characters, str_len):
    in_progress_name = []
    next_letter = ""
    while not next_letter == "+" and len(in_progress_name) < 30:
        # prep the data to run in the model again
        in_progress_name_padded = in_progress_name[-str_len:]
        in_progress_name_padded = (
            list((str_len - len(in_progress_name_padded)) * "*") + in_progress_name_padded
        )
        in_progress_name_numeric = [characters.index(char) for char in in_progress_name_padded]
        in_progress_name_tensor = torch.tensor(in_progress_name_numeric)
        in_progress_name_tensor = torch.nn.functional.one_hot(
            in_progress_name_tensor, num_classes=len(characters)
        ).float()
        in_progress_name_tensor = torch.unsqueeze(in_progress_name_tensor, 0)

        # get the probabilities of each possible next character by running the model
        with torch.no_grad():
            next_letter_probabilities = model(in_progress_name_tensor)

        next_letter_probabilities = next_letter_probabilities[0, -1, :]
        next_letter_probabilities = (
            torch.nn.functional.softmax(next_letter_probabilities, dim=0).detach().cpu().numpy()
        )
        next_letter_probabilities = next_letter_probabilities[1:]
        next_letter_probabilities = [
            p / sum(next_letter_probabilities) for p in next_letter_probabilities
        ]

        # determine what the actual letter is
        next_letter = characters[
            np.random.choice(len(characters) - 1, p=next_letter_probabilities) + 1
        ]
        if next_letter != "+":
            # if the next character isn't stop add the latest generated character to the name and continue
            in_progress_name.append(next_letter)
    # turn the list of characters into a single string
    pet_name = "".join(in_progress_name).title()
    return pet_name

def train(rh):
    num_epochs = 25
    batch_size = 16384

    worker_rank = int(dist.get_rank())
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


    logger.info(f"Worker {worker_rank} - beginning")

    dataset = OurDataset(pet_names, device=device)
    # the distributed sampler makes it so the samples are distributed across the different workers
    sampler = DistributedSampler(dataset)
    loader = DataLoader(dataset, batch_size=batch_size, sampler=sampler)
    worker_rank = int(dist.get_rank())

    # the model has to both be passed to the GPU device, then has to be wrapped in DDP so it can communicate with the other workers
    model = Model()
    model = model.to(device)
    model = DDP(model)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    for epoch in range(num_epochs):
        # the logger here logs to the Dask log of each worker, for easy debugging
        logger.info(
            f"Worker {worker_rank} - {datetime.datetime.now().isoformat()} - Beginning epoch {epoch}"
        )

        # this ensures the data is reshuffled each epoch
        sampler.set_epoch(epoch)
        dataset.permute()

        # nothing in the code for each batch is now any different than base PyTorch
        for i, (batch_x, batch_y) in enumerate(loader):
            optimizer.zero_grad()
            batch_y_pred = model(batch_x)

            loss = criterion(batch_y_pred.transpose(1, 2), batch_y)
            loss.backward()
            optimizer.step()

            logger.info(
                f"Worker {worker_rank} - {datetime.datetime.now().isoformat()} - epoch {epoch} - batch {i} - batch complete - loss {loss.item()}"
            )

        # the first rh call saves a json file with the loss from the worker at the end of the epoch
        rh.submit_result(
            f"logs/data_{worker_rank}_{epoch}.json",
            json.dumps(
                {
                    "loss": loss.item(),
                    "time": datetime.datetime.now().isoformat(),
                    "epoch": epoch,
                    "worker": worker_rank,
                }
            ),
        )
        # this saves the model. We only need to do it for one worker (so we picked worker 0)
        if worker_rank == 0:
            rh.submit_result("model.pkl", pickle.dumps(model.state_dict()))

def main(pet_names, mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file):

    # Prepare output dirs
    timestr = time.strftime("%Y%m%d-%H%M%S")
    stdout = sys.stdout
    Dir = timestr+"-RUN/"
    ReportDir = Dir+"Reports/"
    ResultDir = Dir+"Results/"
    NormalizedDir = ResultDir+"Normalized/"
    LabeledDir = ResultDir+"Labeled/"
    ThresholdDir = ResultDir+"Threshold/"
    [os.mkdir(d) for d in [Dir, ReportDir, ResultDir, NormalizedDir, LabeledDir, ThresholdDir]]
    os.environ['DARSHAN_LOG_DIR_PATH'] = "./"

    client = validate(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file)
    key = uuid.uuid4().hex
    rh = results.DaskResultsHandler(key)
    print(rh)
    futures = dispatch.run(client, train, rh, backend='gloo')
    rh.process_results("/eagle/radix-io/agueroudji/training/", futures, raise_errors=False)

    
    # load the model and the trained parameters
    model_state = pickle.load(open("/eagle/radix-io/agueroudji/training/model.pkl", "rb"))
    model = torch.nn.DataParallel(Model())
    model.load_state_dict(model_state)

    # Generate 50 names then filter out existing ones
    generated_names = [generate_name(model, characters, str_len) for i in range(0, 50)]
    generated_names = [name for name in generated_names if name not in pet_names]
    print(generated_names)

    time.sleep(3) #time to clean data
    client.shutdown()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument('--mode',
                        action='store',
                        dest='mode',
                        type=str,
                        help='Lauching mode, LocalCluster by default, it can be MPI using dask-mpi, or Distributed where a scheduler-file is required')

    parser.add_argument('--yappi',
                        action='store',
                        dest='yappi_config',
                        type=str,
                        help='Activate yappi profiler, by default None, it can be set to wall or cpu time')

    parser.add_argument('--dask-perf-report',
                        action='store',
                        dest='dask_perf_report',
                        type=str,
                        help='Generate Dask performance report in this file path')

    parser.add_argument('--task-graph',
                        action='store',
                        dest='task_graph',
                        type=str,
                        help='None by default, if mentioned it corresponds to filename of the task-graph')

    parser.add_argument('--task-stream',
                        action='store',
                        dest='task_stream',
                        type=str,
                        help='None by default, if mentioned it corresponds to filename of the task-stream')
    parser.add_argument('--scheduler-file',
                        action='store',
                        dest='Scheduler_file',
                        type=str,
                        help='Scheduler file path')


    args = parser.parse_args()
    print(f'Received Mode = {args.mode}, Yappi = {args.yappi_config}, Dask_performance_report = {args.dask_perf_report} Task_graph = {args.task_graph}, Task_stream = {args.task_stream}, Scheduler_file = {args.Scheduler_file}')

    t0 = time.time()
    with urllib.request.urlopen(
    "https://saturn-public-data.s3.us-east-2.amazonaws.com/examples/pytorch/seattle_pet_licenses_cleaned.json"
    ) as f:
        pet_names = json.loads(f.read().decode("utf-8"))

    # Our list of characters, where * represents blank and + represents stop
    characters = list("*+abcdefghijklmnopqrstuvwxyz-. ")
    str_len = 8

    main(pet_names, args.mode, args.yappi_config, args.dask_perf_report, args.task_graph, args.task_stream, args.Scheduler_file)
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")


sys.exit(0)

