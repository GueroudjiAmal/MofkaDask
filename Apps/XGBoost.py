import sys
import os

import dask
from   distributed import Client
import time
import yaml

from collections.abc import Iterator
from datetime import datetime

import dask.array as da
import dask.dataframe as dd
import distributed
import numpy as np
import xgboost
from dask_ml.metrics import mean_squared_error
import pandas as pd


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

# Here we subset data for cross-validation
def make_cv_splits(ddf,
    n_folds: int = 5, ) -> Iterator[tuple[dd.DataFrame, dd.DataFrame]]:
    frac = [1 / n_folds] * n_folds
    splits = ddf.random_split(frac, shuffle=True)
    for i in range(n_folds):
        train = [splits[j] for j in range(n_folds) if j != i]
        test = splits[i]
        yield dd.concat(train), test

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

    client = validate(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file)
    # Main workflow
    ddf = dd.read_parquet("/grand/radix-io/agueroudji/data/Parquets")
    # Under the hood, XGBoost converts floats to `float32`.
    float_cols = ddf.select_dtypes(include="float").columns.tolist()
    ddf = ddf.astype({c: np.float32 for c in float_cols})

    object_cols = ddf.select_dtypes(include="object").columns.tolist()
    ddf = ddf.astype({c: "category" for c in object_cols})

    object_cols = ddf.select_dtypes(include="string").columns.tolist()
    ddf = ddf.astype({c: "category" for c in object_cols})

    dtime_cols = ddf.select_dtypes(include="datetime64[ns]").columns.tolist()
    for c in dtime_cols:
        ddf[c] = ddf[c].apply(lambda x: x.value)

    categorical_vars = ddf.select_dtypes(include="category").columns.tolist()

    # categorize() reads the whole input and then discards it.
    # Let's read from disk only once.
    ddf = ddf.persist()
    
    ddf = ddf.categorize(columns=categorical_vars)

    # We will need to access this multiple times. Let's persist it.
    ddf = ddf.persist()

    start = datetime.now()
    scores = []

    for i, (train, test) in enumerate(make_cv_splits(ddf)):
        print(f"Training/Test split #{i}")
        y_train = train["trip_time"]
        X_train = train.drop(columns=["trip_time"])
        y_test = test["trip_time"]
        X_test = test.drop(columns=["trip_time"])

        print("Building DMatrix...")
        d_train = xgboost.dask.DaskDMatrix(
            None, X_train, y_train, enable_categorical=True
        )

        print("Training model...")
        model = xgboost.dask.train(
            None,
            {"tree_method": "hist"},
            d_train,
            num_boost_round=4,
            evals=[(d_train, "train")],
        )

        print("Running model on test data...")
        predictions = xgboost.dask.predict(None, model, X_test)

        print("Measuring accuracy of model vs. ground truth...")
        score = mean_squared_error(
            y_test.to_dask_array(),
            predictions.to_dask_array(),
            squared=False,
            compute=False,
        )
        # Compute predictions and mean squared error for this iteration
        # while we start the next one
        scores.append(score.reshape(1).persist())
        print("-" * 80)
        print(model)

    scores = da.concatenate(scores).compute()
    print(f"RSME={scores.mean()} +/- {scores.std()}")
    print(f"Total time:  {datetime.now() - start}")

    # Output distributed Configuration
    with open(ReportDir + "distributed.yaml", 'w') as f:
        yaml.dump(dask.config.get("distributed"),f)
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

