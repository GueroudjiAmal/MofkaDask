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

from distributed.diagnostics.plugin import WorkerPlugin

class MofkaWorkerPlugin(WorkerPlugin):
    """
    MofkaWorkerPlugin is a plugin that couples Dask distributed to Mofka through the worker.
    This plugin pushes information about the progress and state transition of Dask tasks in
    the worker.
    """
    def __init__(self, worker, mofka_protocol, ssg_file):
        logging.basicConfig(filename="MofkaWorkerPlugin.log",
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # create mofka client
        self.worker = worker
        self.engine = Engine(mofka_protocol, use_progress_thread=True)
        self.client = mofka.Client(self.engine.mid)
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
        producer_name = "Dask_worker_producer"
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(1)
        ordering = mofka.Ordering.Strict
        self.producer = self.topic.producer(producer_name, batchsize, thread_pool, ordering)
        logging.info("Mofka producer %s is created", producer_name)


    def setup(self, worker):
        """
        Run when the plugin is attached to a worker. This happens when the plugin is registered
        and attached to existing workers, or when a worker is created after the plugin has been
        registered.
        """
        # XXX
        self.worker = worker

    def teardown(self, worker):
        """Run when the worker to which the plugin is attached is closed, or
        when the plugin is removed."""
        teardown = str({"time" : time.time()}).encode("utf-8")
        f = self.producer.push({"action": "teardown"}, teardown)
        f.wait()
        self.producer.flush()
        del self.producer
        del self.topic
        del self.service
        del self.client
        del self.engine

    def transition(
        self,
        key,
        start,
        finish,
        **kwargs: Any,
    ):
        """
        Throughout the lifecycle of a task (see :doc:`Worker State
        <worker-state>`), Workers are instructed by the scheduler to compute
        certain tasks, resulting in transitions in the state of each task. The
        Worker owning the task is then notified of this state transition.

        Whenever a task changes its state, this method will be called.

        .. warning::

            This is an advanced feature and the transition mechanism and details
            of task states are subject to change without deprecation cycle.

        Parameters
        ----------
        key :
        start :
            Start state of the transition.
            One of waiting, ready, executing, long-running, memory, error.
        finish :
            Final state of the transition.
        kwargs :
            More options passed when transitioning
        """
        transition_data = str({"key"            : str(key),
                               "start"          : start,
                               "finish"         : finish,
                               "called_from"    : self.worker.name,
                               "time"           : time.time()
                               }).encode("utf-8")
        f = self.producer.push({"action": "worker_transition"}, transition_data)
        f.wait()
        self.producer.flush()


@click.command()
@click.option('--mofka-protocol',
                type=str,
                default="na+sm",
                help="Mofka protocol",)
@click.option('--ssg-file',
               type=str,
               default="mofka.ssg",
               help="Mofka ssg file path")

async def dask_setup(worker, mofka_protocol, ssg_file):
    plugin = MofkaWorkerPlugin(worker, mofka_protocol, ssg_file)
    await worker.plugin_add(plugin)
