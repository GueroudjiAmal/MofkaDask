import sys
import os

from   distributed import Client
import time
import yaml

import MDAnalysis as mda
import numpy as np
import pmda
import dask
from MDAnalysisData import datasets
from pmda import rms
from pmda import custom
from pmda import rdf


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

def asp_finder(asp_sod, cutoff):
    # Generate AtomGroup of binding sites
    sites = asp_sod.select_atoms('resname ASP and around {} (resname SOD)'.format(
           cutoff))
    # Generate the list of the resids of binding sites
    ix = np.unique(np.array(sites.residues.ix))
    return ix

def Z_SOD(ag):
    Z = [ag.positions[i][2] for i in range(ag.n_atoms)]
    T = [ag.universe.trajectory.time]
    return np.append(T, Z)

def main(nhaa, mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file):

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

    client = validate(mode, yappi_config, dask_perf_report, task_graph, task_stream, scheduler_file)

    #nhaa = datasets.fetch_nhaa_equilibrium()

    # Main workflow

    u = mda.Universe(nhaa.topology, nhaa.trajectory)
    ca = u.select_atoms('name CA')
    u.trajectory[0]
    ref = u.select_atoms('name CA')
    rmsd = rms.RMSD(ca, ref)
    rmsd.run(n_jobs=4, n_blocks=4)
    print("rmsd", rmsd.rmsd)
    SOD = u.select_atoms('resname SOD')
    parallel_z = custom.AnalysisFromFunction(Z_SOD, u, SOD)
    parallel_z.run(n_jobs=4)
    print("parallel_z", parallel_z.results)
    ASP = u.select_atoms('resname ASP')
    O_ASP = ASP.select_atoms('name OD1 or name OD2')
    rdf_SOD_O = rdf.InterRDF(SOD, O_ASP, nbins=100, range=(0.0, 5.0))
    rdf_SOD_O.run(n_jobs=4)
    print("rdf_SOD_O.bins", rdf_SOD_O.bins)
    print("rdf_SOD_O.rdf", rdf_SOD_O.rdf)

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
    parser.add_argument('--scheduler-file',
                        action='store',
                        dest='Scheduler_file',
                        type=str,
                        help='Scheduler file path')


    args = parser.parse_args()
    print(f'Received Mode = {args.mode}, Yappi = {args.yappi_config}, Dask_performance_report = {args.dask_perf_report} Task_graph = {args.task_graph}, Task_stream = {args.task_stream}, Scheduler_file = {args.Scheduler_file}')
    nhaa = datasets.fetch_nhaa_equilibrium()
    t0 = time.time()
    main(nhaa, args.mode, args.yappi_config, args.dask_perf_report, args.task_graph, args.task_stream, args.Scheduler_file)
    print(f"\n\nTotal time taken  = {time.time()-t0:.2f}s")


sys.exit(0)

