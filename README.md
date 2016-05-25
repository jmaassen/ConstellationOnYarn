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

To compile this example, clone it from github: 

	git clone git@github.com:jmaassen/ConstellationOnYarn.git

Ensure hadoop is installed on the machine you are using and _$HADOOP_HOME_ is set: 

	echo $HADOOP_HOME

This should return a value such as '/cm/shared/package/hadoop/hadoop-2.5.0'. Next compile using ant:

	ant

To start the application the _ApplicationSubmitter_ must be started. This will run on the local machine and contact the YARN scheduler to submit the _ApplicationMaster_: 

	java -cp $HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:./dist/example1.jar \
                nl.esciencecenter.constellation.example1.ApplicationSubmitter \
                wordcount/input/data.txt \
                nl.esciencecenter.constellation.example1.ApplicationMaster \
                $PWD/dist/example1.jar \
                $PWD/lib \
                1



git pull ; ant clean ; ant

export CLASSPATH=$CLASSPATH:`/cm/shared/package/hadoop/hadoop-2.5.0/bin/yarn classpath`
java nl.esciencecenter.constellation.example1.ApplicationSubmitter /user/jason/hansken/images/00000000-0000-0000-0000-000000000001 nl.esciencecenter.constellation.example1.ApplicationMaster ./dist/example1.jar ./lib 1

/cm/shared/package/hadoop/hadoop-2.5.0/bin/yarn logs -applicationId application_1448873059693_0292 | less
/cm/shared/package/hadoop/hadoop-2.5.0/bin/yarn application -kill application_1448873059693_0291







