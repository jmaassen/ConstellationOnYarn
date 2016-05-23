/**
 * Copyright 2016 Netherlands eScience Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.esciencecenter.constellation.example1;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.MultiEventCollector;
import ibis.constellation.SimpleExecutor;
//import ibis.constellation.SingleEventCollector;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitExecutorContext;
import ibis.ipl.server.Server;
import ibis.util.TypedProperties;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {

    private static Constellation cn;
    private static MultiEventCollector sec;
    private static ActivityIdentifier secid;

    private static long start;
    private static long end;

    // Bit of a hack to get logging output in a location we want
    private static PrintWriter log;

    private static final void submitJobs(String inputFile) throws Exception { 

        // Find the test input file. 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Path testfile = new Path("wordcount/input/data.txt"); 
        Path testfile = new Path(inputFile); 

        if (!fs.exists(testfile)) { 
            throw new Exception("Could not find input file!");
        }

        FileStatus stat = fs.getFileStatus(testfile);

        log.println("Found input file " + testfile.getName() + " with length " + stat.getLen() + " blocksize " + 
                stat.getBlockSize() + " replication " + stat.getReplication());

        BlockLocation [] locs = fs.getFileBlockLocations(testfile, 0, stat.getLen());

        // Sumbit collector job here to collect replies
        log.println("Submitting event collector");

        sec = new MultiEventCollector(new UnitActivityContext("master"), locs.length);
        secid = cn.submit(sec);

        // Generate a Job for each block
        log.println("Block locations: ");

        int index = 0;

        for (BlockLocation b : locs) { 
            log.println("Block " + b.getOffset() + " - " + (b.getOffset() + b.getLength()));
            log.println("Block locations: " + Arrays.toString(b.getHosts()));
            log.println("Cached locations: " + Arrays.toString(b.getCachedHosts()));
            log.println("Names: " + Arrays.toString(b.getNames()));
            log.println("Topo paths: " + Arrays.toString(b.getTopologyPaths()));

            log.println("Submitting TestJob " + index);

            cn.submit(new TestJob(secid, new UnitActivityContext("test"), inputFile, index));
        }

    }

    private static final void waitForJobs() throws Exception { 

        Event [] events = sec.waitForEvents();

        log.println("Result : " + Arrays.toString(events));

        cn.done();

        end = System.currentTimeMillis();

        log.println("Constellation test run took: " + (end-start) + " ms.");
    }

    private static final void startConstellation(String address) throws Exception { 

        log.println("Starting Constellation");     

        start = System.currentTimeMillis();

        int exec = 1;

        Executor [] e = new Executor[exec];

        StealStrategy st = StealStrategy.ANY;

        for (int i=0;i<exec;i++) { 
            e[i] = new SimpleExecutor(StealPool.WORLD, StealPool.WORLD, new UnitExecutorContext("master"), st, st, st);
        }

        Properties p = new Properties();
        p.put("ibis.constellation.master", "true");
        p.put("ibis.pool.name", "test"); 
        p.put("ibis.server.address", address);

        cn = ConstellationFactory.createConstellation(p, e);
        cn.activate();

        long init = System.currentTimeMillis();

        log.println("Constellation test init took: " + (init-start) + " ms.");
    }

    public static void main(String[] args) throws Exception {

        // Copy input parameters here:
        // 
        // hdfs path to input file, relative to hdfs home of user
        // executor main class
        // full path to application jar
        // full library path
        // container count
        // full path to local log file

        String inputFile = args[0];
        String mainClass = args[1];
        String appJar = args[2];
        String libPath = args[3];
        int n = Integer.parseInt(args[4]);    
        String logfile = args[5];       

        // We create a log file here in a user specified location. Simply printing will produce logs the hadoop directory tree.       
        File f = new File(logfile);
        f.createNewFile();

        log = new PrintWriter(f);

        try { 
            log.println("ApplicationMaster started " + Arrays.toString(args));

            // Initialize clients to ResourceManager and NodeManagers
            Configuration conf = new YarnConfiguration();

            AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
            rmClient.init(conf);
            rmClient.start();

            NMClient nmClient = NMClient.createNMClient();
            nmClient.init(conf);
            nmClient.start();

            // Register with ResourceManager
            log.println("registerApplicationMaster 0");
            rmClient.registerApplicationMaster("", 0, "");
            log.println("registerApplicationMaster 1");

            Resource res = rmClient.getAvailableResources();

            log.println("Available resources: " + res);

            // Priority for worker containers - priorities are intra-application
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);

            // Resource requirements for worker containers
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(128);
            capability.setVirtualCores(1);

            // Make container requests to ResourceManager
            for (int i = 0; i < n; ++i) {
                ContainerRequest containerAsk = new ContainerRequest(capability, /*String [] nodes*/null, /*String [] racks*/null, 
                        priority);

                log.println("Making res-req " + i);
                rmClient.addContainerRequest(containerAsk);
            }

            Map<String, String> appMasterEnv = new HashMap<String, String>();

            StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }

            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

            // Add the runtime classpath needed for tests to work
            if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
                classPathEnv.append(':');
                classPathEnv.append(System.getProperty("java.class.path"));
            }

            classPathEnv.append(':');
            // classPathEnv.append("/home/jason/Workspace/ConstellationOnYarn/dist/simpleapp.jar");
            classPathEnv.append(appJar);

            // File libdir = new File("/home/jason/Workspace/ConstellationOnYarn/lib");
            File libdir = new File(libPath);
            File [] libs = libdir.listFiles();

            for (File l : libs) { 
                if (l.isFile()) { 
                    classPathEnv.append(':');
                    classPathEnv.append(l.getCanonicalPath());
                }
            }

            appMasterEnv.put("CLASSPATH", classPathEnv.toString());

            // Start an Ibis server here, to serve the pool of constellations.
            TypedProperties properties = new TypedProperties();
            properties.putAll(System.getProperties());

            Server server = new Server(properties);          
            String address = server.getAddress();

            log.println("Started server at: " + address);

            // Start a Constellation here that only serves as a source of jobs and sink of results. 

            log.println("Starting Constellation");

            startConstellation(address);

            log.println("Submitting Jobs");

            submitJobs(inputFile);

            log.println("Launching containers");

            // Obtain allocated containers, launch executors and check for responses
            int responseId = 0;
            int completedContainers = 0;

            int launchedContainers = 0;

            LinkedList<AllocateResponse> resp = new LinkedList<>();

            while (launchedContainers < n) {
                AllocateResponse response = rmClient.allocate(responseId++);

                for (Container container : response.getAllocatedContainers()) {
                    // Launch container by create ContainerLaunchContext
                    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                    log.println("Starting container on : " + container.getNodeId().getHost());

                    ctx.setCommands(
                            Collections.singletonList(
                                    Environment.JAVA_HOME.$$() + "/bin/java" +
                                            " -Xmx256M" +
                                            " -Dibis.pool.name=test" + 
                                            " -Dibis.server.address=" + address +
                                            " " + mainClass + 
                                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/executor.stdout" + 
                                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/executor.stderr" 
                                    ));

                    ctx.setEnvironment(appMasterEnv);

                    log.println("Launching container " + container.getId());

                    launchedContainers++;

                    nmClient.startContainer(container, ctx);

                    resp.add(response);

                    Thread.sleep(100);                
                }            
            }

            log.println("Waiting for Job result from " + resp.size() + " containers");

            waitForJobs();

            log.println("Waiting for containers");

            while (completedContainers < n) {
                for (AllocateResponse response : resp) {
                    for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                        ++completedContainers;
                        log.println("Completed container " + status.getContainerId());
                    }
                }

                Thread.sleep(100);
            }

            log.println("Cleanup");

            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "We rule!!", "");
            rmClient.stop();

            // Kill the ibis server
            server.end(-1);

        } catch (Exception e) {
            log.println("Failed " + e);
            e.printStackTrace(log);         
        } finally { 
            log.close();
        }

    }
}
