import os.path
import time


def file_exists(file_path):
    while not os.path.exists(file_path):
        time.sleep(3)

    if os.path.isfile(file_path):
        return True
    else:
        raise ValueError("%s isn't a file!" % file_path)

def my_data_selector(metadata, descriptor):
    return descriptor

def my_data_broker(metadata, descriptor):
    data = bytearray(descriptor.size)
    return [data]