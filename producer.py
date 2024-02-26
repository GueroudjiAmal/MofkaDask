import time

import ctypes
from pymargo.core import Engine
from pymargo.core import client as client_mode
import pymofka_client as mofka

from distributed import Client
import dask
import pyssg

def add(a, b):
    return a + b

def main(scheduler_file, protocol, ssg_file):
    pyssg.init()

    client = mofka.Client(Engine(protocol, mode=client_mode).mid)
    service = client.connect(ssg_file)

    # open a topic
    name = "task_states"
    topic = service.open_topic(name)

    # create a producer
    batchsize = mofka.AdaptiveBatchSize
    thread_pool = mofka.ThreadPool(1)
    ordering = mofka.Ordering.Strict
    producer = topic.producer("my_producer", batchsize, thread_pool, ordering)

    c = Client(scheduler_file=scheduler_file)
    f = c.submit(add, 2, 3)
    print("this is a dask future", f, flush=True)
    r = f.result()
    print("My result", r, flush=True)
    print("Done", flush=True)

    ft = producer.push({"key": str(f)}, str(r).encode('ascii'))
    ft.wait()
    print("we have pushed the data", "key: ", f, "res: ", r, flush=True)
    producer.flush()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument('--scheduler_file',
                        action='store',
                        dest='scheduler_file',
                        type=str,
                        help='Scheduler file path')

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
    main(args.scheduler_file, args.protocol, args.ssg_file)
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")


sys.exit(0)
