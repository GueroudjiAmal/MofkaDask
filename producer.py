import time
import sys

from pymargo.core import Engine
from pymargo.core import server as server_mode
import pymofka_client as mofka

from distributed import Client
import dask
import pyssg

import dask.array as da
from utils import file_exists
import click


def add(a, b):
    return a + b

def mul(a, b):
    return a * b

def mofkatask(a, b, mofka_protocol, ssg_file):
    "example of ceating a mofka Task"
    engine = Engine(mofka_protocol, use_progress_thread=True)
    client = mofka.Client(engine.mid)
    pyssg.init()
    service = client.connect(ssg_file)

    # create or open a topic
    try:
        name = "Numerics"
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
    r = a + b
    f = producer.push({"action": "get_result"}, r.to_bytes(8, byteorder='big'))
    f.wait()
    producer.flush()
    return r

@click.command()
@click.option('--scheduler-file',
                type=str,
                default="",
                help="Dask scheduler file",)
@click.option('--mofka-protocol',
                type=str,
                default="na+sm",
                help="Mofka protocol",)
@click.option('--ssg-file',
               type=str,
               default="mofka.ssg",
               help="Mofka ssg file path")
def main(scheduler_file, mofka_protocol, ssg_file):
    t0 = time.time()
    # Create a Dask Client
    c = Client(scheduler_file=scheduler_file)
    # Submit computations
    a = da.random.random((1024, 1024), chunks=(256, 256))
    b = da.random.random((1024, 1024), chunks=(256, 256))
    a = add(a, b)
    m = mul(a, b)
    k = m.max() - a.min() * m.max() - m.min()*a
    k = k.mean()
    f = c.compute(k)
    r = f.result()
    print("The computed result is :", r, flush=True)

    # # This is to test without MofkaWorkerPlugin running,
    # f = c.submit(mofkatask, 1024, 2048, mofka_protocol, ssg_file)
    # r = f.result()
    # print("The computed result in mofkatask is :", r, flush=True)

    print("Done", flush=True)
    # c.shutdown()

    """
    To push data from the dask client to mofka uncomment the following lines
    """
    """
    engine = Engine(mofka_protocol, use_progress_thread=True)
    client = mofka.Client(engine.mid)
    pyssg.init()
    file_exists(ssg_file)
    service = client.connect(ssg_file)

    # create or open a topic
    try:
        name = "Numerics"
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

    f = producer.push({"action": "get_result"}, r.data)
    f.wait()
    producer.flush()
    """
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s", flush=True)

if __name__ == '__main__':
    main()

sys.exit(0)
