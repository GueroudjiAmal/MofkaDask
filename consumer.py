import os
import sys
import json
import time

from pymargo.core import Engine
from pymargo.core import client as client_mode
import pymofka_client as mofka
from  custom import my_broker_selector
import pyssg


def main(protocol, ssg_file):
    client = mofka.Client(Engine(protocol).mid)
    pyssg.init()
    service = client.connect(ssg_file)

    # open a topic
    name = "task_states"
    topic = service.open_topic(name)

    # Create a consumer
    batchsize = mofka.AdaptiveBatchSize
    thread_pool = mofka.ThreadPool(1)
    ordering = mofka.Ordering.Strict
    consumer = topic.consumer("my_consumer", batchsize, thread_pool, my_broker_selector.broker, my_broker_selector.selector, topic.partitions)

    f = consumer.pull()
    event = f.wait()
    metadata = json.loads(event.metadata)
    bool = True
    print(metadata, flush=True)
    try:
        if metadata["action"] == "stop":
            bool = False
    except:
        pass
    while bool:
        f = consumer.pull()
        event = f.wait()
        data = event.data
        metadata = event.metadata
        print("Data", data, flush=True)
        print("metadata", metadata, flush=True)
        try:
            if metadata["action"] == "stop":
                bool = False
            print("boool", bool, flush=True)
        except:
            pass

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
