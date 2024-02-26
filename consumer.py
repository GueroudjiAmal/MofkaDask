import os
import sys
import json
import time

from pymargo.core import Engine
from pymargo.core import client as client_mode
import pymofka_client as mofka
import pyssg

def main(protocol, ssg_file):
    pyssg.init()
    client = mofka.Client(Engine(protocol, mode=client_mode).mid)
    service = client.connect(ssg_file)

    # open a topic
    name = "task_states"
    topic = service.open_topic(name)

    # Create a consumer
    batchsize = mofka.AdaptiveBatchSize
    thread_pool = mofka.ThreadPool(1)
    ordering = mofka.Ordering.Strict
    consumer = topic.consumer("my_consumer", batchsize, thread_pool, my_broker_selector.broker, my_broker_selector.selector, topic.partitions)
    print("consumer created", consumer, flush=True)
    f = consumer.pull()
    print("consumer pull", f, flush=True)
    event = f.wait()
    print("event", event, flush=True)
    data = event.data
    ptr = data.segments[0].ptr
    size = data.segments[0].size
    ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.c_char_p
    ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    ptr = ctypes.pythonapi.PyCapsule_GetPointer(ptr, None)
    data = ctypes.c_char_p(ptr).value
    print("I am the consumer here is the data", data)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument('--protocol',
                        action='store',
                        dest='protocol',
                        type=str,
                        help='Protocol')

    parser.add_argument('--ssg_file',
                        action='store',
                        dest='ssg_file',
                        type=str,
                        help='SSG file path')

    args = parser.parse_args()
    t0 = time.time()
    main(args.protocol, args.ssg_file)
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")


sys.exit(0)
