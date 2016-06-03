# ConstellationOnYarn

This repo contains some experimental code for putting the Consellation runtime system on YARN/HDFS

The `nl.esciencecenter` package contains a simple example application that reads an input file and computes a hash of each block.

Prerequisites
-------------

To run this example you need a machine which has the following software installed: 

- Java 1.7 or higher
- ant 1.7 or higher
- Hadoop 2.5 or higher 

Note that Hadoop must be set up properly, i.e., the `$HADOOP_HOME` environment variable must be set up. In addition, make sure you 
use the Java version that is used to run Hadoop to compile this example. Using a different version may result in unsupported version 
exceptions.

Compilation
-----------

After cloning this example, change to the directory and compile using:

	ant

This compiles the example, and creates a `./dist` directory containing a `ConstellationOnYARN.jar` plus all dependencies.

Running the example
-------------------

To run the example you will first need to put an input file in HDFS. As usual with Hadoop, bigger is better. You can use an ubuntu source 
image for example: 

	wget http://ftp.acc.umu.se/mirror/cdimage.ubuntu.com/releases/xenial/release/source/ubuntu-16.04-src-1.iso

Next put this image into HDFS for example (after replacing `/user/jason` with your home directory in HDFS): 

	hdfs dfs -copyFromLocal ubuntu-16.04-src-1.iso /user/jason
  
Next run the example using (after replacing `/user/jason` with your home directory in HDFS __twice__): 

	java -cp ./dist/ConstellationOnYarn.jar:`yarn classpath` nl.esciencecenter.ConstellationOnYarn /user/jason/ ./dist /user/jason/ubuntu-16.04-src-1.iso

This starts the `nl.esciencecenter.ConstellationOnYarn` example _locally_, which connects to the YARN scheduler and submits the example. This class 
expects the following command-line parameters:

- The HDFS root directory to which the dependencies should be staged-in, `/user/jason` in this example.
- The local directory containing the dependencies that need to be staged in. Use `./dist` to run this example.
- The input file (on HDFS) to process, `/user/jason/ubuntu-16.04-src-1.iso` in this example. 
- The number of YARN Workers to submit, `1` in this example.

The example first copies the `./dist` directory to HDFS, as these files are needed on the YARN nodes to run the example. It then stubmits the job to YARN, 
and waits for it to finish.

The example code
----------------  

The example code can be found in 'src' and consists of the following packages:

- `nl.esciencecenter` the main example code
- `nl.esciencecenter.constellation` the constellation application 
- `nl.esciencecenter.yarn` a somewhat generic library to access YARN 
    
Like most YARN applications, running this application requires three parts: 

- `nl.esciencecenter.ConstellationSubmitter`: the _main_ application that connects to the YARN scheduler and submits the ApplicationMaster 
- `nl.esciencecenter.ApplicationMaster`: the _master_ application that runs on a YARN node, aquires the nodes to run one or more `nl.esciencecenter.constellation.ConstellationWorker` applications and starts them, starts a `nl.esciencecenter.constellation.ConstellationMaster`, and waits for everything to finish.
- `nl.esciencecenter.constellation.ConstellationWorker`: the _worker_ application that is started on the YARN nodes to processes one or more `nl.esciencecenter.constellation.SHA1Job`s.

In addition, the following classes are used:

- `nl.esciencecenter.constellation.ConstellationMaster`: the Constellation master application which starts a Constellation, submits the jobs, and gathers the results.
- `nl.esciencecenter.constellation.SHA1Job`: a job that is created by the ConstellationMaster and executed on one of the ConstellationWorkder. Each of these jobs reads a block of data from a file in HDFS, and calculates the SHA1 hash of this block.
- `nl.esciencecenter.constellation.SHA1Result`: a result object that is returned by each SHA1Job to the ConstellationMaster.
- `nl.esciencecenter.yarn.YarnSubmitter`: a generic class used by 'ConstellationOnYarn' to submit the application to YARN.
- `nl.esciencecenter.yarn.YarnMaster`: a generic class used by 'ApplicationMaster' to submit the workers to YARN.
- `nl.esciencecenter.yarn.Utils`: various utility methods used by 'ApplicationSubmitter' and 'ApplicationMaster'.

