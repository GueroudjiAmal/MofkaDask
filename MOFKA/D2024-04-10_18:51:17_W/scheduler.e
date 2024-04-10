2024-04-10 18:51:45,876 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-10 18:51:46,095 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-10 18:51:46,096 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-10 18:51:46,104 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-10 18:51:46,104 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-10 18:51:46,110 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-10 18:51:47,138 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-10 18:51:47,178 - distributed.scheduler - INFO - State start
2024-04-10 18:51:47,182 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-10 18:51:47,201 - distributed.scheduler - INFO -   Scheduler at:    tcp://10.201.2.39:8786
2024-04-10 18:51:47,202 - distributed.scheduler - INFO -   dashboard at:  http://10.201.2.39:8787/status
2024-04-10 18:51:47,204 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-10 18:51:48,056 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-10 18:51:53,649 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.1.190:39643', status: init, memory: 0, processing: 0>
2024-04-10 18:51:55,667 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.1.190:39643
2024-04-10 18:51:55,667 - distributed.core - INFO - Starting established connection to tcp://10.201.1.190:56992
2024-04-10 18:51:55,668 - distributed.scheduler - INFO - Receive client connection: Client-649082e4-f76b-11ee-98f3-6805cae1ddbc
2024-04-10 18:51:55,669 - distributed.core - INFO - Starting established connection to tcp://10.201.2.40:56898
2024-04-10 18:52:00,354 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-10 18:52:00,354 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-10 18:52:00,355 - distributed.core - INFO - Connection to tcp://10.201.1.190:56992 has been closed.
2024-04-10 18:52:00,355 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.1.190:39643', status: running, memory: 1, processing: 0> (stimulus_id='handle-worker-cleanup-1712775120.3551304')
2024-04-10 18:52:00,355 - distributed.scheduler - WARNING - Removing worker 'tcp://10.201.1.190:39643' caused the cluster to lose already computed task(s), which will be recomputed elsewhere: {'finalize-2ae637a13f5025640c0ffae4e35da517'} (stimulus_id='handle-worker-cleanup-1712775120.3551304')
