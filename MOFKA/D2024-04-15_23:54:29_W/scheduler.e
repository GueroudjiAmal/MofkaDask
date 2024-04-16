2024-04-15 23:56:31,534 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-15 23:56:31,588 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-15 23:56:31,589 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-15 23:56:31,593 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-15 23:56:31,593 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-15 23:56:31,599 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-15 23:56:32,655 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-15 23:56:32,695 - distributed.scheduler - INFO - State start
2024-04-15 23:56:32,698 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-15 23:56:32,717 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.1.89:8786
2024-04-15 23:56:32,717 - distributed.scheduler - INFO -   dashboard at:  http://10.201.1.89:8787/status
2024-04-15 23:56:32,720 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-15 23:56:33,576 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-15 23:56:36,021 - distributed.scheduler - INFO - Receive client connection: Client-cafef4cd-fb83-11ee-bebd-6805cad947f6
2024-04-15 23:56:36,578 - distributed.core - INFO - Starting established connection to tcp://10.201.3.101:43526
2024-04-15 23:56:41,772 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.141:38865', status: init, memory: 0, processing: 0>
2024-04-15 23:56:41,772 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.141:38865
2024-04-15 23:56:41,772 - distributed.core - INFO - Starting established connection to tcp://10.201.2.141:59016
2024-04-15 23:56:41,773 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.141:42477', status: init, memory: 0, processing: 0>
2024-04-15 23:56:41,773 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.141:42477
2024-04-15 23:56:41,773 - distributed.core - INFO - Starting established connection to tcp://10.201.2.141:59018
2024-04-15 23:56:41,774 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.141:34351', status: init, memory: 0, processing: 0>
2024-04-15 23:56:41,774 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.141:34351
2024-04-15 23:56:41,774 - distributed.core - INFO - Starting established connection to tcp://10.201.2.141:59034
2024-04-15 23:56:41,775 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.2.141:36055', status: init, memory: 0, processing: 0>
2024-04-15 23:56:41,775 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.2.141:36055
2024-04-15 23:56:41,775 - distributed.core - INFO - Starting established connection to tcp://10.201.2.141:59038
2024-04-15 23:58:17,884 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.38s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-15 23:59:22,443 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.18s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-15 23:59:34,437 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.92s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-16 00:00:43,139 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.35s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-16 00:00:43,148 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-16 00:00:43,150 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-16 00:00:43,150 - distributed.core - INFO - Connection to tcp://10.201.2.141:59016 has been closed.
2024-04-16 00:00:43,150 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.141:38865', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713225643.1508417')
2024-04-16 00:00:43,626 - distributed.core - INFO - Connection to tcp://10.201.2.141:59018 has been closed.
2024-04-16 00:00:43,626 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.141:42477', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713225643.6262512')
2024-04-16 00:00:43,626 - distributed.core - INFO - Connection to tcp://10.201.2.141:59034 has been closed.
2024-04-16 00:00:43,626 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.141:34351', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713225643.6268108')
2024-04-16 00:00:43,627 - distributed.core - INFO - Connection to tcp://10.201.2.141:59038 has been closed.
2024-04-16 00:00:43,627 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.2.141:36055', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713225643.6273072')
2024-04-16 00:00:43,627 - distributed.scheduler - INFO - Lost all workers
2024-04-16 00:00:43,629 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.1.89:8786'
2024-04-16 00:00:43,629 - distributed.scheduler - INFO - End scheduler
