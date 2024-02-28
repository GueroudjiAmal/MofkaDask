import time
import sys

from pymargo.core import Engine
from pymargo.core import server as server_mode
import pymofka_client as mofka

from distributed import Client
import dask
import pyssg

import dask.array as da

def add(a, b):
    return a + b

def mul(a, b):
    return a * b

def main(scheduler_file, protocol, ssg_file):
    engine = Engine(protocol, use_progress_thread=True)
    client = mofka.Client(engine.mid)
    pyssg.init()
    service = client.connect(ssg_file)

    c = Client(scheduler_file=scheduler_file)
    # create a topic
    try:
        name = "task_states"
        validator = mofka.Validator.from_metadata({"__type__":"my_validator:./custom/libmy_validator.so"})
        selector = mofka.PartitionSelector.from_metadata({"__type__":"my_partition_selector:./custom/libmy_partition_selector.so"})
        serializer = mofka.Serializer.from_metadata({"__type__":"my_serializer:./custom/libmy_serializer.so"})
        service.create_topic(name, validator, selector, serializer)
        service.add_memory_partition(name, 0)
    except:
        pass
    topic = service.open_topic(name)

    # create a producer
    batchsize = mofka.AdaptiveBatchSize
    thread_pool = mofka.ThreadPool(1)
    ordering = mofka.Ordering.Strict
    producer = topic.producer("my_producer", batchsize, thread_pool, ordering)
    print("Producer Created", producer, flush=True)

    a = da.random.random((10, 10))
    b = mul(a, 1000)
    f = c.submit(add, a, b)
    print("this is a dask future", f, flush=True)
    r = f.result().compute()
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
