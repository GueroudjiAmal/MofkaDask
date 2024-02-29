import os
import sys
import json
import time

from pymargo.core import Engine
from pymargo.core import client as client_mode
import pymofka_client as mofka
import pyssg


def selector(metadata, descriptor):
    return descriptor

def broker(metadata, descriptor):
    data = bytearray(descriptor.size)
    return [data]

def main(protocol, ssg_file):

    client = mofka.Client(Engine(protocol).mid)
    pyssg.init()
    service = client.connect(ssg_file)
    # open a topic
    name = "Dask"
    topic = service.open_topic(name)

    # Create a consumer
    consumer = topic.consumer("my_consumer", batch_size=1, data_broker=broker, data_selector=selector)

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
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++", flush=True)
        print("Metadata ", metadata, "Data ", data[0].decode("utf-8", "replace"), flush=True)
        try:
            if metadata["action"] == "stop":
                bool = False
                exit
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
