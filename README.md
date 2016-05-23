# ConstellationOnYarn

This repo contains some experimental code for putting the Consellation RTS on YARN/HDFS

The _nl.esciencecenter.example1_ package contains a simple example application that reads an input file and computes a hash of each block.

The application consists of three three parts:

- ApplicationMaster: the YARN master application that aquires and manages the resources, starts the workers, submits the jobs, and gathers for the results.
- ExecutorMain: the worker application that is started on the YARN nodes and runs one or more jobs.
- TestJob: the job that runs on the workers, reads a block of data from a file, and calculates the hash.

