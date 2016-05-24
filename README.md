# ConstellationOnYarn

This repo contains some experimental code for putting the Consellation RTS on YARN/HDFS

The _nl.esciencecenter.example1_ package contains a simple example application that reads an input file and computes a hash of each block.

Like most YARN applications, it consists of three parts: 

- ApplicationSubmitter: the application that connects to the YARN scheduler and submits the ApplicationMaster 
- ApplicationMaster: the 'master' application that runs on a YARN node, aquires the nodes to run one or more ExecutorMain applications, submits the jobs, and gathers the results.
- ExecutorMain: the 'worker' application that is started on the YARN nodes and processes one or more jobs.

In addition, the following classes are used:

- SHA1Job: a job that is created by the ApplicationMaster and executed on one of the ExecutorMain instances. Each of these jobs reads a block of data from a file in HDFS, and calculates the SHA1 hash of this block.
- SHA1Result: a result object that is returned by each SHA1Job to the ApplicationMaster.






