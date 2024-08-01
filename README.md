# Mofka-Dask Coupler

There are several possibilities to couple Dask and Mofka, depending on what/how/why the user wants to make the
two systems communicate.

- **By using the same python script for Dask client and mofka producer:**
  This is the easiest solution, and the less sofisticated. It can be used for example to push results from Dask to Mofka

- **By using Scheduler plugins:**
  This solution corresponds to implement a class that inherits from the SchedulerPlugin interface in Dask.
  The implemented methods will be called everytime the corresponding methods is called in the scheduler.

  For instance, when the transition method is called, we push few peices of data to Mofka, including the key
  of the task, its old and crurrent states as well as the timestamp.

  This has been shown in the `MofkaSchedulerPlugin.py` file. The goal is to push to Mofka all transitions that happen
  is the scheduler.

  The plugin solution implies that mofka clients will leave as long as the sceduler is alive (or the worker for WoerkerPlugin).

- **By using Wokrer plugins:**
  Similarly to the scheduler plugins, this implements a class ingeriting for the WorkerPlugin interface.
  This has been shown in the `MofkaWorkerPlugin.py` file.

- **By using Mofka tasks:**
  This consits of a stateless coupling. The idea is to have a Mofka producer created inside a Dask task, have it push the wanted
  data, then it disappears with the task.

  It may be interesting when we want to push to Mofka few peices of data rarely (if not this may cause unwanted overhead).

- **By using Dask Actors:**
  This is a statefull solution. It is similar to the WorkerPlugin one as the Mofka producer lives in the worker. However in this solution,
  the mofka Producer would be created within another object called Actor. This Actor can be instanciated in a specific worker rather than all
  of them.
  There may be interesting usecases for this as well.


# How it works
Here are the steps to run Mofka-Dask Coupler:

## Set up a Mofka server:

It can be done using command line:
`bedrock na+sm -c config.json`
This will create a group file called "mofka.json" here.

## Launch the Dask scheduler with the Mofka plugin:

`dask scheduler --scheduler-file=scheduler.json --preload plugins/MofkaSchedulerPlugin.py --mofka-protocol=na+sm --group-file=mofka.json`

Connection information of the Dask scheduler will be written into `scheduler.json`

The plugin takes two arguments:

 - `mofka-protocol` : which is specified while creating the mofka server, for instance `na+sm`
 - `group-file` : which is the path to the group file created while seting up the mofka server, here `mofka.json`

## Launch the Dask workers with the Mofka plugin:

`dask worker --scheduler-file=scheduler.json --preload plugins/MofkaWorkerPlugin.py --mofka-protocol=na+sm --group-file=mofka.json`

## Lauch the Dask Client:

`python producer.py  --scheduler-file=scheduler.json --mofka-protocol=na+sm --group-file=mofka.json`

In this example we both have Dask Client and Mofka producer in the same script to showcase the fist coupling possibility.

## Launch a Mofka Consumer:

`python consumer.py --mofka-protocol=na+sm --group-file=mofka.json`

This consumer only pocesses data pushed from the plugins.

