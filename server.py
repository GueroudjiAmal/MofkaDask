import os
import sys
import time


import ctypes
from pymargo.core import Engine
from mochi.bedrock.server import Server as BedrockServer

import pymofka_client as mofka
from  custom import my_broker_selector


def main(protocol, config_file):

    config = open(config_file).read()
    bedrock_server = BedrockServer(protocol, config)
    mid = bedrock_server.margo.mid
    gid = bedrock_server.ssg["mofka_group"].handle
    client = mofka.Client(mid=mid)
    service = client.connect(gid)

    # create a topic
    name = "task_states"
    validator = mofka.Validator({"__type__":"my_validator:./custom/libmy_validator.so"})
    selector = mofka.PartitionSelector({"__type__":"my_partition_selector:./custom/libmy_partition_selector.so"})
    serializer = mofka.Serializer({"__type__":"my_serializer:./custom/libmy_serializer.so"})
    service.create_topic(name, validator, selector, serializer)
    service.add_partition(name, 0)
    topic = service.open_topic(name)

    time.sleep(100000)
    del topic
    del service
    del client
    del thread_pool
    bedrock_server.finalize()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument('--protocol',
                        action='store',
                        dest='protocol',
                        type=str,
                        help='Protocol')

    parser.add_argument('--config_file',
                        action='store',
                        dest='config_file',
                        type=str,
                        help='Config file path')

    args = parser.parse_args()
    t0 = time.time()
    main(args.protocol, args.config_file)
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")

sys.exit(0)
