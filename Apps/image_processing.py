import sys
import os
import subprocess

import dask_image.imread
import dask_image.ndfilters
import dask_image.ndmeasure
import dask
import dask.array as da
import numpy as np
from   skimage import io

from   distributed import Client, performance_report
import time

import yaml

def grayscale(rgb):
    result = ((rgb[..., 0] * 0.2125) +
              (rgb[..., 1] * 0.7154) +
              (rgb[..., 2] * 0.0721))
    return result

def save_file(arr, repo, pattern, block_info=None):
    filename = repo + pattern + "-".join(map(str, block_info[0]["chunk-location"])) + ".png"
    arr = arr.astype(np.uint8)
    io.imsave(filename, arr)
    return arr

def validate(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file):

    # Validate mode
    if mode == "MPI":
        from dask_mpi import initialize
        initialize()
        client = Client()
    elif mode == "distributed":
        if scheduler_file:
            client= Client(scheduler_file = scheduler_file)
        else:
            raise ValueError("When distributed Mode is activated the the path to the scheduler-file must be specified, current value is %s: " % scheduler_file)
    elif mode == "LocalCluster" or mode is None:
        client = Client(processes=False)
    else:
        raise ValueError("Unknown launching mode %s" % mode)

    return client


def processing(image):

    # Grayscale
    grayscaled = grayscale(image)

    # Filtering
    smoothed_image = dask_image.ndfilters.gaussian(grayscaled, sigma=[1, 1])

    # Segmentation
    threshold_value = 0.75 * da.max(smoothed_image)
    threshold_image =  smoothed_image > threshold_value
    label_image, num_labels = dask_image.ndmeasure.label(threshold_image)

    return {"label_image": label_image[:1024, :2028], "threshold_image": threshold_image[:2048, :2048]}

def main(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file):

    # Prepare output dirs
    timestr = time.strftime("%Y%m%d-%H%M%S")
    stdout = sys.stdout
    Dir = timestr+"-RUN/"
    ReportDir = Dir+"Reports/"
    ResultDir = Dir+"Results/"
    NormalizedDir = ResultDir+"Normalized/"
    LabeledDir = ResultDir+"Labeled/"
    ThresholdDir = ResultDir+"Threshold/"
    [os.mkdir(d) for d in [Dir, ReportDir, ResultDir, NormalizedDir, LabeledDir, ThresholdDir]]
    os.environ['DARSHAN_LOG_DIR_PATH'] = "./"

    filename_pattern = os.path.join("/home/agueroudji/Dataset/images", '*.png')
    client = validate(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file)
    # Main workflow

    images = dask_image.imread.imread(filename_pattern)
    #images = images[10]
    print(images)
    try:
        normalized_images = (images - da.min(images)) * (255.0 / (da.max(images) - da.min(images)))
        print(normalized_images)
        normalized_images.map_blocks(save_file, NormalizedDir, "normalized-" , dtype=normalized_images.dtype, enforce_ndim=False).compute()
    except Exception as e:
        print("There was an excp ", e )

    print("Normalized  images: ", normalized_images)

    results = [processing(im) for im in normalized_images]
    label_images = [r["label_image"] for r in results]
    threshold_image = [r["threshold_image"] for r in results]

    label_images = da.stack(label_images)
    threshold_image = da.stack(threshold_image)

    try:
        label_images.map_blocks(save_file, LabeledDir, "labeled-", dtype=label_images.dtype, enforce_ndim=False).compute()
    except Exception as e:
        print("There was an excp ", e )
    try:
        threshold_image.map_blocks(save_file, ThresholdDir, "threshold-", dtype=threshold_image.dtype, enforce_ndim=False).compute()
    except Exception as e:
        print("There was an excp ", e )
    #client.retire_workers(client.scheduler_info()['workers'])

    # Output distributed Configuration
    with open(ReportDir + "distributed.yaml", 'w') as f:
        yaml.dump(dask.config.get("distributed"),f)
    del threshold_image 
    del label_image
    del normalized_images
    client.retire_workers(client.scheduler_info()['workers'])
    time.sleep(3) #time to clean data  
    client.shutdown()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument('--mode',
                        action='store',
                        dest='mode',
                        type=str,
                        help='Lauching mode, LocalCluster by default, it can be MPI using dask-mpi, or Distributed where a scheduler-file is required')

    parser.add_argument('--yappi',
                        action='store',
                        dest='yappi_config',
                        type=str,
                        help='Activate yappi profiler, by default None, it can be set to wall or cpu time')

    parser.add_argument('--dask-perf-report',
                        action='store',
                        dest='dask_perf_report',
                        type=str,
                        help='Generate Dask performance report in this file path')

    parser.add_argument('--task-graph',
                        action='store',
                        dest='task_graph',
                        type=str,
                        help='None by default, if mentioned it corresponds to filename of the task-graph')

    parser.add_argument('--task-stream',
                        action='store',
                        dest='task_stream',
                        type=str,
                        help='None by default, if mentioned it corresponds to filename of the task-stream')
    parser.add_argument('--scheduler-file',
                        action='store',
                        dest='Scheduler_file',
                        type=str,
                        help='Scheduler file path')


    args = parser.parse_args()
    print(f'Received Mode = {args.mode}, Yappi = {args.yappi_config}, Dask_performance_report = {args.dask_perf_report} Task_graph = {args.task_graph}, Task_stream = {args.task_stream}, Scheduler_file = {args.Scheduler_file}')

    t0 = time.time()
    main(args.mode, args.yappi_config, args.dask_perf_report, args.task_graph, args.task_stream, args.Scheduler_file)
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")


sys.exit(0)
