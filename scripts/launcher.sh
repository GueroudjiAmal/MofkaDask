ml load gcc/12.2.0 PrgEnv-gnu cudatoolkit-standalone libfabric/1.15.2.0 cray-mpich/8.1.25
source  ~/spack/share/spack/setup-env.sh
spack env activate mofkadask

DIR=$PWD
echo $DIR

RUNS=100
NWORKERS=4

for R in 1  #{1..$RUNS}
do
    NNODES=$(($NWORKERS / 4 + 4)) #2 workers per node, one node for client and one for scheduler one for Mofka consumer and one for mofka server
    mkdir -p MOFKA
    DATE=$(date +"%Y-%m-%d_%T")
    WORKSPACE=$DIR/MOFKA/D${DATE}_W${NWORKER}/
    mkdir  -p $WORKSPACE
    cd $WORKSPACE
    cp -r  $DIR/*.py  $DIR/Apps/*.py $DIR/scripts/* $DIR/*.json $DIR/*txt $DIR/custom $DIR/plugins/* .
    echo Running in $WORKSPACE
    qsub -A radix-io -l select=$NNODES:system=polaris -o $WORKSPACE polaris.sh
done
