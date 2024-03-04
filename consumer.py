import os
import sys
import time

from pymargo.core import Engine
from pymargo.core import client as client_mode
import pymofka_client as mofka
import pyssg

from utils import file_exists, my_data_selector, my_data_broker
import logging

import pandas as pd
import csv
import click


class MofkaConsumer():

    def __init__(self, mofka_protocol, ssg_file):
        logging.basicConfig(filename="MofkaConsumer.log",
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filemode='w')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        self.engine = Engine(mofka_protocol, use_progress_thread=True)
        self.client = mofka.Client(self.engine.mid)
        pyssg.init()
        logger.info("Mofka client created")

        file_exists(ssg_file)

        self.service = self.client.connect(ssg_file)
        logger.info("Mofka service created")
        topic_name = "Dask"
        try:
            validator = mofka.Validator.from_metadata({"__type__":"my_validator:./custom/libmy_validator.so"})
            selector = mofka.PartitionSelector.from_metadata({"__type__":"my_partition_selector:./custom/libmy_partition_selector.so"})
            serializer = mofka.Serializer.from_metadata({"__type__":"my_serializer:./custom/libmy_serializer.so"})
            self.service.create_topic(topic_name, validator, selector, serializer)
            self.service.add_memory_partition(topic_name, 0)
            logger.info("Mofka topic %s is created", topic_name)
        except:
            logger.info("Topic %s already exists", topic_name)
            pass

        self.topic = self.service.open_topic(topic_name)
        logger.info("Mofka topic %s is opened by consumer", topic_name)

        # Create a consumer
        consumer_name = "Dask_consumer"
        self.consumer = self.topic.consumer(name=consumer_name,
                                batch_size=1,
                                data_broker=my_data_broker,
                                data_selector=my_data_selector)
        logger.info("Mofka consumer %s is created", consumer_name)

        self.transition_rec = pd.DataFrame()
        self.client_rec = pd.DataFrame()
        self.worker_rec = pd.DataFrame()
        self.graph_rec = pd.DataFrame()
        self.stop = False

    def append_event_data(self, metadata , data):

        if metadata["action"] == "transition":
            if self.transition_rec.empty:
                self.transition_rec = pd.DataFrame.from_records([data])
            else:
                self.transition_rec = pd.concat([self.transition_rec,
                                                 pd.DataFrame.from_records([data])],
                                                 ignore_index=True)

        elif metadata["action"] == "update_graph":
            if self.graph_rec.empty:
                self.graph_rec = pd.DataFrame.from_records([data])
            else:
                self.graph_rec = pd.concat([self.graph_rec,
                                            pd.DataFrame.from_records([data])],
                                            ignore_index=True)

        elif metadata["action"] == "remove_worker" or metadata["action"] == "add_worker" :
            if self.worker_rec.empty:
                self.worker_rec = pd.DataFrame.from_records([data])
            else:
                self.worker_rec = pd.concat([self.worker_rec,
                                             pd.DataFrame.from_records([data])],
                                             ignore_index=True)

        elif metadata["action"] == "add_client":
            if self.client_rec.empty:
                self.client_rec = pd.DataFrame.from_records([data])
            else:
                self.client_rec = pd.concat([self.client_rec,
                                             pd.DataFrame.from_records([data])],
                                             ignore_index=True)

        elif metadata["action"] == "remove_client":
            if self.client_rec.empty:
                self.client_rec = pd.DataFrame.from_records([data])
            else:
                self.client_rec = pd.concat([self.client_rec,
                                             pd.DataFrame.from_records([data])],
                                             ignore_index=True)
            self.stop = True
        elif metadata["action"] == "close" or metadata["action"] == "before_close" : self.stop = True


    def get_data(self):
        while not (self.stop):
            f = self.consumer.pull()
            event = f.wait()
            data = event.data[0].decode("utf-8", "replace")
            data = eval(data)
            metadata = eval(event.metadata)
            self.append_event_data(metadata, data)

    def teardown(self):
        self.transition_rec.to_csv("transition.csv")
        self.client_rec.to_csv("client.csv")
        self.worker_rec.to_csv("worker.csv")
        self.graph_rec.to_csv("graph.csv")

@click.command()
@click.option('--mofka-protocol',
                type=str,
                default="na+sm",
                help="Mofka protocol",)
@click.option('--ssg-file',
               type=str,
               default="mofka.ssg",
               help="Mofka ssg file path")
def main(mofka_protocol, ssg_file):
    consumer = MofkaConsumer(mofka_protocol, ssg_file)
    consumer.get_data()
    consumer.teardown()


if __name__ == '__main__':
    t0 = time.time()
    main()
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")


sys.exit(0)
