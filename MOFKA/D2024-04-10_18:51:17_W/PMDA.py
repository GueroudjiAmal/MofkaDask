import sys
import os

from   distributed import Client, performance_report
import time
import yappi

import yaml

import MDAnalysis as mda
import numpy as np
import pmda
import dask
from MDAnalysisData import datasets
import nglview as nv
from pmda import rms


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

    # Validate yappi configuration
    if yappi_config == "wall" or yappi_config is None:
        yappi.set_clock_type("WALL")
    elif yappi_config == "cpu":
        yappi.set_clock_type("CPU")
    else:
        raise ValueError("Unknown mode for yappi, please specify CPU for cpu time, and WALL for Wall time")

    # Validate Dask performance report
    if dask_perf_report is None:
        dask_perf_report = "dask_perf_report.html"

    # Valide task stream file
    if task_stream is None:
        task_stream= "task_stream.yaml"

    # Validate task graph file
    if task_graph is None:
        task_graph = "task_graph.out"

    return client, dask_perf_report, task_graph, task_stream


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
    os.environ['DARSHAN_LOG_DIR_PATH'] = ReportDir

    client, dask_perf_report, task_graph, task_stream = validate(mode, yappi_config, dask_perf_report,
                                                                task_graph, task_stream, scheduler_file)

    nhaa = datasets.fetch_nhaa_equilibrium()

    # Main workflow
    with (
        performance_report(filename=ReportDir+dask_perf_report) as dask_perf,
        yappi.run(),
    ):

        u = mda.Universe(nhaa.topology, nhaa.trajectory)
        ca = u.select_atoms('name CA')
        u.trajectory[0]
        ref = u.select_atoms('name CA')
        rmsd = rms.RMSD(ca, ref)
        rmsd.run(n_jobs=4, n_blocks=4)
        # TODO print rmsd.rmsd
        # add

    # Output Reports for yappi
    yappi.get_func_stats().save(ReportDir+"yappi_callgrind.prof", type="callgrind")
    yappi.get_func_stats().save(ReportDir+"yappi_pstat.prof", type="pstat")

    with open(ReportDir + "yappi_debug.yaml", 'w') as f:
        sys.stdout = f
        yaml.dump(yappi.get_func_stats().debug_print())
        sys.stdout = stdout

    # Output task stream
    with open(ReportDir + task_stream, 'w') as f:
        yaml.dump(client.get_task_stream(), f)

    # Output distributed Configuration
    with open(ReportDir + "distributed.yaml", 'w') as f:
        yaml.dump(dask.config.get("distributed"),f)

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
    parser.add_argument('--scheduler_file',
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
