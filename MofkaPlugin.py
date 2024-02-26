import os
import sys
import json
import string
import random
import subprocess
import tempfile
import time
import click
wd = os.getcwd()
sys.path.insert(0, '../../build/tests/python/cpputils/')

import ctypes
from pymargo.core import Engine
from mochi.bedrock.server import Server as BedrockServer
import pymofka_client as mofka
import my_broker_selector

from distributed.diagnostics.plugin import SchedulerPlugin

import signal

def signal_handler(sig, frame):
    print('Ctrl+C')
    sys.exit(0)

class MofkaPlugin(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        bedrock_config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "config.json")
        config = open(bedrock_config_file).read()
        self.bedrock_server = BedrockServer("na+sm", config)
        mid = self.bedrock_server.margo.mid
        gid = self.bedrock_server.ssg["mofka_group"].handle
        self.client = mofka.Client(mid=mid)
        service = self.client.connect(gid)

        # create a topic
        name = "task_states"
        validator = mofka.Validator({"__type__":"my_validator:../../build/tests/python/cpputils/libmy_validator.so"})
        selector = mofka.PartitionSelector({"__type__":"my_partition_selector:../../build/tests/python/cpputils/libmy_partition_selector.so"})
        serializer = mofka.Serializer({"__type__":"my_serializer:../../build/tests/python/cpputils/libmy_serializer.so"})
        service.create_topic(name, validator, selector, serializer)
        service.add_partition(name, 0)
        topic = service.open_topic(name)

        # create a producer
        self.producer = topic.producer("my_producer")

        # create a consumer
        batchsize = mofka.AdaptiveBatchSize
        thread_pool = mofka.ThreadPool(random.randint(1,8))
        ordering = mofka.Ordering.Strict
        self.consumer = topic.consumer("my_consumer", batchsize, thread_pool, my_broker_selector.broker, my_broker_selector.selector, topic.partitions)
        self.data = dict()

    def transition(self, key, start, finish, *args, **kwargs):
        # Get full TaskState
        ts = self.scheduler.tasks[key]
        f = self.producer.push({"key": key}, str(ts).encode('ascii'))
        f.wait()
        print("we have pushed the data", "key: ", key, "ts: ", ts, flush=True)
        self.producer.flush()

        f = self.consumer.pull()
        event = f.wait()
        data = event.data
        ptr = data.segments[0].ptr
        size = data.segments[0].size
        print("I am the consumer here is the data", data.segments , type(ptr), ptr, size)
        ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_char_p
        ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
        ptr = ctypes.pythonapi.PyCapsule_GetPointer(ptr, None)
        data = ctypes.c_char_p(ptr).value
        self.data[key] = data

    def before_close(self):
        #self.bedrock_server.finalize()
        print("here is my data", self.data)

    def close(self):
        print("this is close", self.data)

@click.command()
def dask_setup(scheduler):
    signal.signal(signal.SIGINT, signal_handler)
    plugin = MofkaPlugin(scheduler)
    scheduler.add_plugin(plugin)
