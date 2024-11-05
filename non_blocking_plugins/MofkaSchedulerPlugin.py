import os
import sys
import json
import time
import click
import logging

from pymargo.core import Engine
import mochi.mofka.client as mofka
from typing import Any

from distributed.diagnostics.plugin import SchedulerPlugin

class MofkaSchedulerPlugin(SchedulerPlugin):
    """
    MofkaSchedulerPlugin couples Dask distributed witj Mofka through the Scheduler.
    This plugin pushes information about the progress and state transition of Dask
    tasks in the scheduler, adding/removing clients/workers.
    """
    def __init__(self, scheduler, mofka_protocol, group_file):
        logging.basicConfig(filename="MofkaSchedulerPlugin.log",
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        # create mofka client
        print("about to try to create dask client")
        self.scheduler = scheduler
        self.engine = Engine(mofka_protocol, use_progress_thread=True)
        self.driver = mofka.MofkaDriver(group_file, self.engine)
        print("Created dask clinet")
        # create a topic
        topic_name = "Dask"
        if not self.driver.topic_exists(topic_name):
            logging.info("Mofka topic %s is created by MofkaPlugin", topic_name)
            self.driver.create_topic(topic_name)
            self.driver.add_memory_partition(topic_name, 0)

        self.topic = self.driver.open_topic(topic_name)

        # create a producer
        producer_name = "Dask_scheduler_producer"
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(1)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(producer_name, batchsize, thread_pool, ordering)
        logging.info("Mofka producer %s is created", producer_name)
        

    async def start(self, scheduler):
        """Run when the scheduler starts up

        This runs at the end of the Scheduler startup process
        """
        try:
            f = self.producer.push({"action": "restart", "time" : str(time.time())})
        except Exception as Argument:
            logging.exception("Exception while calling restart method when sending")

    async def before_close(self):
        """Runs prior to any Scheduler shutdown logic"""
        before_close = str({"time" : time.time()}).encode("utf-8")
        try:
            f = self.producer.push({"action": "before_close", "time" : str(time.time())})
            f.wait()
            
            
        except Exception as Argument:
            logging.exception("Exception while calling before_close method when sending")

    async def close(self):
        """Run when the scheduler closes down

        This runs at the beginning of the Scheduler shutdown process, but after
        workers have been asked to shut down gracefully. Given, we are closing 
        the worker, this push is allowed to be blocking.
        """
        close = str({"time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "close"}, close)
        f.wait()

    def update_graph(
        self,
        scheduler,
        client: str,
        keys: set,
        tasks: list,
        annotations: dict,
        priority: dict,
        dependencies: dict,
        **kwargs: Any
    ):
        """Run when a new graph / tasks enter the scheduler

        Parameters
        ----------
            scheduler:
                The `Scheduler` instance.
            client:
                The unique Client id.
            keys:
                The keys the Client is interested in when calling `update_graph`.
            tasks:
                The
            annotations:
                Fully resolved annotations as applied to the tasks in the format::

                    {
                        "annotation": {
                            "key": "value,
                            ...
                        },
                        ...
                    }
            priority:
                Task calculated priorities as assigned to the tasks.
            dependencies:
                A mapping that maps a key to its dependencies.
            **kwargs:
                It is recommended to allow plugins to accept more parameters to
                ensure future compatibility.
        """
        try:
            f = self.producer.push({"action": "update_graph", "client": str(client),
                            "keys": str(keys),
                            "dependencies": str(dependencies),
                            "time": str(time.time())}
                            )
        except Exception as Argument:
            logging.exception("Exception while calling update_graph method when sending")

    def restart(self, scheduler):
        """Run when the scheduler restarts itself"""
        try:
            f = self.producer.push({"action": "restrat", "time": str(time.time())})
        except Exception as Argument:
            logging.exception("Exception while calling restart method when sending")

    def transition(
        self,
        key,
        start,
        finish,
        stimulus_id: str,
        **kwargs: Any):
        """Run whenever a task changes state

        For a description of the transition mechanism and the available states,
        see :ref:`Scheduler task states <scheduler-task-state>`.

        To force the buffer to flush, submit a task with the key "flush-mofka-buffer" as shown below:
        -----
        def no_op():
            return None
            
        client.submit(no_op, key="flush-mofka-buffer")
        ----

        .. warning::

            This is an advanced feature and the transition mechanism and details
            of task states are subject to change without deprecation cycle.

        Parameters
        ----------
        key :
        start :
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish :
            Final state of the transition.
        stimulus_id :
            ID of stimulus causing the transition.
        *args, **kwargs :
            More options passed when transitioning
            This may include worker ID, compute time, etc.
        """
        startstops = None
        begins = None
        ends = None
        duration = None
        size = None
        thread = None
        worker = None
        if kwargs.get("startstops"):
            startstops = kwargs["startstops"][0]
            begins = startstops["start"]
            ends = startstops["stop"]
            duration = startstops["stop"] - startstops["start"]

        if kwargs.get("thread"):
            thread = kwargs["thread"]

        if kwargs.get("nbytes"):
            size = kwargs["nbytes"]

        if kwargs.get("worker"):
            worker = kwargs["worker"]
        
            

        
        try:
            f = self.producer.push( {
                "action": "scheduler_transition",
                "key"            : str(key),
                "thread"         : thread,
                "worker"         : worker,
                "prefix"         : self.scheduler.tasks[key].prefix.name,
                "group"          : self.scheduler.tasks[key].group.name,
                "start"          : start,
                "finish"         : finish,
                "stimulus_id"    : stimulus_id,
                "called_from"    : self.scheduler.address,
                "begins"         : begins,
                "ends"           : ends,
                "duration"       : duration,
                "size"           : size,
                "time"           : str(time.time())}
                
                )
        except Exception as Argument:
            logging.exception("Exception while calling transition method when sending")

        # on custom key, force buffer to flush
        if "flush-mofka-buffer" == str(key):            
            logging.info("Mofka producer started flush")
            self.producer.flush()
            logging.info("Mofka producer completed flush")


    def add_worker(self, scheduler, worker: str):
        """Run when a new worker enters the cluster

        If this method is synchronous, it is immediately and synchronously executed
        without ``Scheduler.add_worker`` ever yielding to the event loop.
        If it is asynchronous, it will be awaited after all synchronous
        ``SchedulerPlugin.add_worker`` hooks have executed.

        .. warning::

            There are no guarantees about the execution order between individual
            ``SchedulerPlugin.add_worker`` hooks and the ordering may be subject
            to change without deprecation cycle.
        """
        try:
            f = self.producer.push({"action": "add_worker",  "time": str(time.time())})
        except Exception as Argument:
            logging.exception("Exception while calling add_worker method when sending")

    def remove_worker(
        self, scheduler, worker: str, stimulus_id: str, **kwargs):
        """Run when a worker leaves the cluster

        If this method is synchronous, it is immediately and synchronously executed
        without ``Scheduler.remove_worker`` ever yielding to the event loop.
        If it is asynchronous, it will be awaited after all synchronous
        ``SchedulerPlugin.remove_worker`` hooks have executed.

        .. warning::

            There are no guarantees about the execution order between individual
            ``SchedulerPlugin.remove_worker`` hooks and the ordering may be subject
            to change without deprecation cycle.
        """
        try:
            f = self.producer.push({"action": "remove_worker", "worker" : worker, "stimulus_id" : stimulus_id,
                         "time" : str(time.time())})

        except Exception as Argument:
            logging.exception("Exception while calling remove_worker method when sending")

    def add_client(self, scheduler, client: str):
        """Run when a new client connects"""
        try:
            f = self.producer.push({"action": "add_client", "client" : client,
                          "time" : str(time.time())})
        except Exception as Argument:
            logging.exception("Exception while calling add_client method when sending")

    def remove_client(self, scheduler, client: str):
        """Run when a client disconnects"""
        try:
            f = self.producer.push({"action": "remove_client", "client" : client, "time" : str(time.time())})
        except Exception as Argument:
            logging.exception("Exception while calling remove client method when sending")

    def log_event(self, topic: str, msg: Any):
        """Run when an event is logged"""
        try:
            f = self.producer.push({"action": "log_event", "topic" : str(topic), "message": str(msg), "time": str(time.time())})

        except Exception as Argument:
            logging.exception("Exception while calling log_event method when sending")

    # TODO It maybe interesting to add to SchedulerPlugin inetface support for other methods.
    # def send_task_to_worker(self, worker: str, ts: TaskState, duration: float = -1):
    # def handle_task_finished(self, ...):
    # def other_handlers(...)


@click.command()
@click.option('--mofka-protocol',
                type=str,
                default="cxi",
                help="Mofka protocol",)
@click.option('--group-file',
               type=str,
               default="mofka.json",
               help="Mofka group file path")

def dask_setup(scheduler, mofka_protocol, group_file):
    plugin = MofkaSchedulerPlugin(scheduler, mofka_protocol, group_file)
    scheduler.add_plugin(plugin)
