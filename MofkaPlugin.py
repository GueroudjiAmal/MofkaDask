import os
import sys
import json
import time
import click

import ctypes
from pymargo.core import Engine
import pymofka_client as mofka
from custom import my_broker_selector
from typing import TYPE_CHECKING, Any, Callable, ClassVar
import pyssg

from distributed.diagnostics.plugin import SchedulerPlugin, WorkerPlugin
import json

class MofkaPlugin(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.engine = Engine("na+sm", use_progress_thread=True)
        self.client = mofka.Client(self.engine.mid)
        pyssg.init()
        self.service = self.client.connect("mofka.ssg")

        # create a topic
        name = "Dask"
        try:
            validator = mofka.Validator.from_metadata({"__type__":"my_validator:./custom/libmy_validator.so"})
            selector = mofka.PartitionSelector.from_metadata({"__type__":"my_partition_selector:./custom/libmy_partition_selector.so"})
            serializer = mofka.Serializer.from_metadata({"__type__":"my_serializer:./custom/libmy_serializer.so"})
            self.service.create_topic(name, validator, selector, serializer)
            self.service.add_memory_partition(name, 0)
        except:
            pass
        self.topic = self.service.open_topic(name)

        # create a producer
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(1)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer("dask_producer", batchsize, thread_pool, ordering)

    async def start(self, scheduler):
        """Run when the scheduler starts up

        This runs at the end of the Scheduler startup process
        """
        restart = {"time" : time.time()}
        self.producer.push({"action": "restart"}, str(restart).encode("ascii"))

    async def before_close(self):
        """Runs prior to any Scheduler shutdown logic"""
        del self.producer
        del self.topic
        del self.service
        del self.client
        del self.engine

    async def close(self):
        """Run when the scheduler closes down

        This runs at the beginning of the Scheduler shutdown process, but after
        workers have been asked to shut down gracefully
        """
        close = {"time" : time.time()}
        self.producer.push({"action": "close"}, str(close).encode("ascii"))

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
        update_graph = {"client": client, "keys": keys, "dependencies": dependencies, "time": time.time()}
        f = self.producer.push({"action": "update_garph"}, str(update_graph).encode('ascii'))
        f.wait()
        print("we have pushed the data", "key: ", keys, flush=True)
        self.producer.flush()

    def restart(self, scheduler):
        """Run when the scheduler restarts itself"""
        restrat = {"time" : time.time()}
        self.producer.push({"action": "restrat"}, str(restart).encode("ascii"))
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
        # Get full TaskState
        ts = self.scheduler.tasks[key]
        f = self.producer.push({"key": key}, str(ts).encode('ascii'))
        transition = {"key" : key, "start": start, "finish" : finish, "stimulus_id" : stimulus_id, "time" : time.time()}
        self.producer.push({"action": "transition"}, str(transition).encode("ascii"))
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
        add_worker = {"worker" : worker, "time" : time.time()}
        self.producer.push({"action": "add_worker"}, str(add_worker).encode("ascii"))
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
        rm_worker = {"worker" : worker, "stimulus_id" : stimulus_id, "time" : time.time()}
        self.producer.push({"action": "remove_worker"}, str(rm_worker).encode("ascii"))
        self.producer.flush()

    def add_client(self, scheduler, client: str):
        """Run when a new client connects"""
        add_client = {"client" : client, "time" : time.time()}
        self.producer.push({"action": "add_client"}, str(add_client).encode("ascii"))
        self.producer.flush()

    def remove_client(self, scheduler, client: str):
        """Run when a client disconnects"""
        rm_client = {"client" : client, "time" : time.time()}
        self.producer.push({"action": "remove_client"}, str(rm_client).encode("ascii"))
        self.producer.flush()

    def log_event(self, topic: str, msg: Any):
        """Run when an event is logged"""
        log_event = {"topic" : topic, "message": msg, "time": time.time()}
        self.producer.push({"action": "log_event"}, str(log_event).encode("ascii"))
        self.producer.flush()

@click.command()
def dask_setup(scheduler):
    plugin = MofkaPlugin(scheduler)
    scheduler.add_plugin(plugin)
