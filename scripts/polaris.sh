#!/bin/bash -l
#PBS -l walltime=0:10:00
#PBS -q debug-scaling
#PBS -A radix-io
#PBS -l filesystems=home:grand:eagle

source  ~/spack/share/spack/setup-env.sh
spack env activate MofkaDask

cd $PBS_O_WORKDIR

SSGFILE=mofka.ssg
SCHEFILE=scheduler.json
CONFIGFILE=config.json
PROTOCOL=na+sm

# Split nodes between the different steps
total=$(wc -l $PBS_NODEFILE | awk '{print $1}')

start_1=1
end_1=1

start_2=$(echo "${end_1}+1" | bc)
end_2=$(echo "${start_2}" | bc)

start_3=$(echo "${end_2}+1" | bc)
end_3=$(echo "${start_3}" | bc)

start_4=$(echo "${end_3}+1" | bc)
end_4=$(echo "${start_4}" | bc)

start_5=$(echo "${end_4}+1" | bc)
end_5=$total

sed -n "${start_1},${end_1}p" $PBS_NODEFILE > SchedulerNode
sed -n "${start_2},${end_2}p" $PBS_NODEFILE > ClientNode
sed -n "${start_3},${end_3}p" $PBS_NODEFILE > WorkerNodes
sed -n "${start_4},${end_4}p" $PBS_NODEFILE > MofkaServer
sed -n "${start_5},${end_5}p" $PBS_NODEFILE > MofkaConsumer

echo Creating Mofka Server

mpiexec  -n 1
         --ppn 1
         -d ${NDEPTH}
         --hostfile MofkaServer bedrock $PROTOCOL -c $CONFIGFILE

# Wait for the SSGFILE to be created
while ! [ -f $SSGFILE ]; do
    sleep 3
    echo -n .
done

echo launching Scheduler
mpiexec  -n 1
         --ppn 1
         -d ${NDEPTH}
         --hostfile SchedulerNode
         --exclusive
         --cpu-bind depth  dask scheduler --scheduler-file=$SCHEFILE
                                          --preload MofkaSchedulerPlugin.py
                                          --mofka-protocol=$PROTOCOL
                                          --ssg-file=$SSGFILE
 1>> scheduler.o  2>> scheduler.e  &

# Wait for the SCHEFILE to be created
while ! [ -f $SCHEFILE ]; do
    sleep 3
    echo -n .
done

# Connect the client to the Dask scheduler
echo Connect Master Client
mpiexec  -n 1
         --ppn 1
         -d ${NDEPTH}
         --hostfile ClientNode
         --exclusive
         --cpu-bind depth  `which python` producer.py --scheduler-file=$SCHEFILE
                                                      --mofka-protocol=$PROTOCOL
                                                      --ssg-file=$SSGFILE 1>> client.o 2>> client.e &

client_pid=$!

# Launch Dask workers in the rest of the allocated nodes
echo Scheduler booted, Client connected, launching workers

NPROC=$((NWORKERS * NRANKS))
mpiexec  -n ${NPROC}
         --ppn ${RANKS}
         -d ${NDEPTH}
         --hostfile WorkerNodes
         --exclusive
         --cpu-bind depth  dask worker  --scheduler-file=$SCHEFILE
                                        --preload MofkaWorkerPlugin.py
                                        --mofka-protocol=$PROTOCOL
                                        --ssg-file=$SSGFILE 1>> worker.o 2>>worker.e  &

# Connect the Mofka consumer client
echo Connect Mofka Client
mpiexec  -n 1
         --ppn 1
         -d ${NDEPTH}
         --hostfile ConsumerNode
         --exclusive
         --cpu-bind depth  `which python` consumer.py --mofka-protocol=$PROTOCOL
                                        -             --ssg-file=$SSGFILE 1>> client.o 2>> client.e &

consumer_pid=$!

# Wait for the client process and Mofka consumer to be finished
wait $client_pid
wait $consumer_pid
wait


                                                                                                                                                                                                 19,1          Top