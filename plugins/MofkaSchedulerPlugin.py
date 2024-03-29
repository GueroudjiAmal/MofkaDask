import os
import sys
import json
import time
import click
import logging

from pymargo.core import Engine
import pymofka_client as mofka
from typing import Any
import pyssg

from distributed.diagnostics.plugin import SchedulerPlugin

from utils import file_exists


class MofkaSchedulerPlugin(SchedulerPlugin):
    """
    MofkaSchedulerPlugin couples Dask distributed witj Mofka through the Scheduler.
    This plugin pushes information about the progress and state transition of Dask
    tasks in the scheduler, adding/removing clients/workers.
    """
    def __init__(self, scheduler, mofka_protocol, ssg_file):
        logging.basicConfig(filename="MofkaSchedulerPlugin.log",
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        # create mofka client
        self.scheduler = scheduler
        self.engine = Engine(mofka_protocol, use_progress_thread=True)
        self.client = mofka.Client(self.engine.mid)
        file_exists(ssg_file)
        pyssg.init()
        self.service = self.client.connect(ssg_file)

        # create a topic
        topic_name = "Dask"
        try:
            validator = mofka.Validator.from_metadata({"__type__":"my_validator:./custom/libmy_validator.so"})
            selector = mofka.PartitionSelector.from_metadata({"__type__":"my_partition_selector:./custom/libmy_partition_selector.so"})
            serializer = mofka.Serializer.from_metadata({"__type__":"my_serializer:./custom/libmy_serializer.so"})
            self.service.create_topic(topic_name, validator, selector, serializer)
            self.service.add_memory_partition(topic_name, 0)
            logging.info("Mofka topic %s is created", topic_name)
        except:
            logging.info("Topic %s already exists", topic_name)
            pass

        self.topic = self.service.open_topic(topic_name)
        logging.info("Mofka topic %s is opened by MofkaPlugin", topic_name)

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
        restart = str({"time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "restart"}, restart)
        f.wait()
        self.producer.flush()

    async def before_close(self):
        """Runs prior to any Scheduler shutdown logic"""
        before_close = str({"time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "before_close"}, before_close)
        f.wait()
        self.producer.flush()


    async def close(self):
        """Run when the scheduler closes down

        This runs at the beginning of the Scheduler shutdown process, but after
        workers have been asked to shut down gracefully
        """
        close = str({"time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "close"}, close)
        f.wait()
        self.producer.flush()
        del self.producer
        del self.topic
        del self.service
        del self.client
        del self.engine

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
        update_graph = str({"client": client,
                            "keys": keys,
                            "dependencies": dependencies,
                            "time": time.time()
                           }).encode("utf-8")
        f = self.producer.push({"action": "update_graph"}, update_graph)
        f.wait()
        self.producer.flush()

    def restart(self, scheduler):
        """Run when the scheduler restarts itself"""
        restrat = str({"time" : time.time()})
        f = self.producer.push({"action": "restrat"}, restart.encode("utf-8"))
        f.wait()
        self.producer.flush()

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
        if kwargs.get("startstops"):
            startstops = kwargs["startstops"]

        transition_data = str({"key"            : str(key),
                               "prefix"         : self.scheduler.tasks[key].prefix.name,
                               "group"          : self.scheduler.tasks[key].group.name,
                               "start"          : start,
                               "finish"         : finish,
                               "stimulus_id"    : stimulus_id,
                               "called_from"    : ("scheduler", self.scheduler.address),
                               "startstops"     : startstops,
                               "time"           : time.time()
                               }).encode("utf-8")
        f = self.producer.push({"action": "transition"}, transition_data)
        f.wait()
        self.producer.flush()

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
        add_worker = str({"worker" : worker, "time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "add_worker"}, add_worker)
        f.wait()
        self.producer.flush()

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
        rm_worker = str({"worker" : worker, "stimulus_id" : stimulus_id,
                         "time" : time.time()
                        }).encode("utf-8")
        f = self.producer.push({"action": "remove_worker"}, rm_worker)
        f.wait()
        self.producer.flush()

    def add_client(self, scheduler, client: str):
        """Run when a new client connects"""
        add_client = str({"client" : client,
                          "time" : time.time()
                        }).encode("utf-8")
        f = self.producer.push({"action": "add_client"}, add_client)
        f.wait()
        self.producer.flush()

    def remove_client(self, scheduler, client: str):
        """Run when a client disconnects"""
        rm_client = str({"client" : client, "time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "remove_client"}, rm_client)
        f.wait()
        self.producer.flush()

    def log_event(self, topic: str, msg: Any):
        """Run when an event is logged"""
        log_event = {"topic" : topic, "message": msg, "time": time.time()}
        f = self.producer.push({"action": "log_event"}, str(log_event).encode("utf-8"))
        f.wait()
        self.producer.flush()

    # TODO It maybe interesting to add to SchedulerPlugin inetface support for other methods.
    # def send_task_to_worker(self, worker: str, ts: TaskState, duration: float = -1):
    # def handle_task_finished(self, ...):
    # def other_handlers(...)


@click.command()
@click.option('--mofka-protocol',
                type=str,
                default="na+sm",
                help="Mofka protocol",)
@click.option('--ssg-file',
               type=str,
               default="mofka.ssg",
               help="Mofka ssg file path")

def dask_setup(scheduler, mofka_protocol, ssg_file):
    plugin = MofkaSchedulerPlugin(scheduler, mofka_protocol, ssg_file)
    scheduler.add_plugin(plugin)
