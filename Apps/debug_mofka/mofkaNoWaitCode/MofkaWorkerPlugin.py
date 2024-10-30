import os
import sys
import json
import time
import click
import logging

from pymargo.core import Engine
import mochi.mofka.client as mofka
from typing import Any

from distributed.diagnostics.plugin import WorkerPlugin

class MofkaWorkerPlugin(WorkerPlugin):
    """
    MofkaWorkerPlugin is a plugin that couples Dask distributed to Mofka through the worker.
    This plugin pushes information about the progress and state transition of Dask tasks in
    the worker.
    """
    def __init__(self, worker, mofka_protocol, group_file):
        logging.basicConfig(filename="MofkaWorkerPlugin.log",
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        # create mofka client
        self.worker = worker
        self.commin = 0
        self.commout = 0
        self.engine = Engine(mofka_protocol, use_progress_thread=True)
        self.driver = mofka.MofkaDriver(group_file, self.engine)

        # create a topic
        topic_name = "Dask"
        if not self.driver.topic_exists(topic_name):
            logging.info("Mofka topic %s is created by MofkaPlugin", topic_name)
            self.driver.create_topic(topic_name)
            self.driver.add_memory_partition(topic_name, 0)

        self.topic = self.driver.open_topic(topic_name)

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
        teardown = {"time" : time.time()}
        try:
            print("worker about to flush", flush=True)
            self.producer.flush()
            
            f = self.producer.push({"action": "remove_worker"}, str(teardown).encode("utf-8"))
            f.wait()
            
        except Exception as Argument:
            logging.exception("Exception while calling remove_worker method when sending", str(teardown))
        
        # del self.producer
        # del self.topic
        # # del self.service
        # del self.client
        # del self.engine

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

        transition_data = str({ "key"            : str(key),
                            "start"          : start,
                            "finish"         : finish,
                            "called_from"    : self.worker.name,
                            "time"           : time.time()
                            }).encode("utf-8")
        try:
            f = self.producer.push({"action": "worker_transition"}, transition_data)
            # f.wait()
        except Exception as Argument:
            logging.exception("Exception while calling transition method when sending", str(transition_data))

        l = self.commin
        l2 = len(self.worker.transfer_incoming_log)
        if l2 > l:
            data = list(self.worker.transfer_incoming_log)[l-1:]
            _ = [e.update({"type": "incoming_transfer", "called_from": self.worker.name, "time": time.time(), "keys": str(e["keys"])}) for e in data]
            self.commin = len(self.worker.transfer_incoming_log)
            for d in data:
                try:
                    dd = str(d).encode("utf-8")
                    f = self.producer.push({"action": "worker_transfer"}, dd)
                    # f.wait()
                except Exception as Argument:
                    logging.exception("Exception while calling transition method when sending", str(d))


        l = self.commout
        l2 = len(self.worker.transfer_outgoing_log)
        if l2 > l:
            data = list(self.worker.transfer_outgoing_log)[l-1:]
            _ = [e.update({"type": "outgoing_transfer", "called_from": self.worker.name, "time": time.time(), "keys" : str(e["keys"])}) for e in data]
            self.commout = len(self.worker.transfer_outgoing_log)
            for d in data:
                try:
                    dd = str(d).encode("utf-8")
                    f = self.producer.push({"action": "worker_transfer"}, dd)
                    # f.wait()
                except Exception as Argument:
                    logging.exception("Exception while calling transition method when sending", str(d))


@click.command()
@click.option('--mofka-protocol',
                type=str,
                default="cxi",
                help="Mofka protocol")
@click.option('--group-file',
               type=str,
               default="mofka.json",
               help="Mofka group file path")

async def dask_setup(worker, mofka_protocol, group_file):
    plugin = MofkaWorkerPlugin(worker, mofka_protocol, group_file)
    await worker.plugin_add(plugin)
