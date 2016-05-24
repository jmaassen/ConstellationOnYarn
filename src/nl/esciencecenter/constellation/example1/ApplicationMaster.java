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
    // private static PrintWriter log;

    private static final void submitJobs(String inputFile) throws Exception { 

        // Find the test input file. 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Path testfile = new Path("wordcount/input/data.txt"); 
        Path testfile = new Path(inputFile); 

     //   if (!fs.exists(testfile)) { 
         //   throw new Exception("Could not find input file " + inputFile + " " + testfile);
        //}

        FileStatus stat = fs.getFileStatus(testfile);

        System.out.println("Found input file " + testfile.getName() + " with length " + stat.getLen() + " blocksize " + 
                stat.getBlockSize() + " replication " + stat.getReplication());

        BlockLocation [] locs = fs.getFileBlockLocations(testfile, 0, stat.getLen());

        // Sumbit collector job here to collect replies
        System.out.println("Submitting event collector");

        sec = new MultiEventCollector(new UnitActivityContext("master"), locs.length);
        secid = cn.submit(sec);

        // Generate a Job for each block
        System.out.println("Block locations: ");

        for (int i=0;i<locs.length;i++) {
            BlockLocation b = locs[i];
            
            System.out.println("Block " + b.getOffset() + " - " + (b.getOffset() + b.getLength()));
            System.out.println("Block locations: " + Arrays.toString(b.getHosts()));
            System.out.println("Cached locations: " + Arrays.toString(b.getCachedHosts()));
            System.out.println("Names: " + Arrays.toString(b.getNames()));
            System.out.println("Topo paths: " + Arrays.toString(b.getTopologyPaths()));

            System.out.println("Submitting TestJob " + i);

            cn.submit(new SHA1Job(secid, new UnitActivityContext("test"), inputFile, i, b.getOffset(), b.getLength()));
        }
    }

    private static final String SHA1toString(byte [] sha1) { 
        
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < sha1.length; i++) {
            sb.append(Integer.toString((sha1[i] & 0xff) + 0x100, 16).substring(1));
        }
         
        return sb.toString();        
    }
    
    private static final void waitForJobs() throws Exception { 

        Event [] events = sec.waitForEvents();

        System.out.println("Results: ");
        
        for (Event e : events) { 
            
            SHA1Result result = (SHA1Result) e.data; 
            
            if (result.hasFailed()) { 
                System.out.println("  " + result.getBlock() + " FAILED");
            } else {             
                System.out.println("  " + result.getBlock() + " " + SHA1toString(result.getSHA1()));
            } 
        }
        
        cn.done();

        end = System.currentTimeMillis();

        System.out.println("Constellation test run took: " + (end-start) + " ms.");
    }

    private static final void startConstellation(String address) throws Exception { 

        System.out.println("Starting Constellation");     

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

        System.out.println("Constellation test init took: " + (init-start) + " ms.");
    }

    public static void main(String[] args) throws Exception {

        // Copy input parameters here:
        // 
        // hdfs path to input file, relative to hdfs home of user
        // full path to application jar
        // full library path
        // container count
        // full path to local log file

        String inputFile = args[0];
        String appJar = args[1];
        String libPath = args[2];
        int n = Integer.parseInt(args[3]);    
        
        try { 
            System.out.println("ApplicationMaster started " + Arrays.toString(args));

            // Initialize clients to ResourceManager and NodeManagers
            Configuration conf = new YarnConfiguration();

            AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
            rmClient.init(conf);
            rmClient.start();

            NMClient nmClient = NMClient.createNMClient();
            nmClient.init(conf);
            nmClient.start();

            // Register with ResourceManager
            System.out.println("registerApplicationMaster 0");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("registerApplicationMaster 1");

            Resource res = rmClient.getAvailableResources();

            System.out.println("Available resources: " + res);

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

                System.out.println("Making res-req " + i);
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

            System.out.println("Started server at: " + address);

            // Start a Constellation here that only serves as a source of jobs and sink of results. 

            System.out.println("Starting Constellation");

            startConstellation(address);

            System.out.println("Submitting Jobs");

            submitJobs(inputFile);

            System.out.println("Launching containers");

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

                    System.out.println("Starting container on : " + container.getNodeId().getHost());

                    ctx.setCommands(
                            Collections.singletonList(
                                    Environment.JAVA_HOME.$$() + "/bin/java" +
                                            " -Xmx256M" +
                                            " -Dibis.pool.name=test" + 
                                            " -Dibis.server.address=" + address +
                                            " nl.esciencecenter.constellation.example1.ExecutorMain " + 
                                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/executor.stdout" + 
                                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/executor.stderr" 
                                    ));

                    ctx.setEnvironment(appMasterEnv);

                    System.out.println("Launching container " + container.getId());

                    launchedContainers++;

                    nmClient.startContainer(container, ctx);

                    resp.add(response);

                    Thread.sleep(100);                
                }            
            }

            System.out.println("Waiting for Job result from " + resp.size() + " containers");

            waitForJobs();

            System.out.println("Waiting for containers");

            while (completedContainers < n) {
                for (AllocateResponse response : resp) {
                    for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                        ++completedContainers;
                        System.out.println("Completed container " + status.getContainerId());
                    }
                }

                Thread.sleep(100);
            }

            System.out.println("Cleanup");

            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "We rule!!", "");
            rmClient.stop();

            // Kill the ibis server
            server.end(-1);

        } catch (Exception e) {
            System.out.println("Failed " + e);
            e.printStackTrace();         
        }
    }
}
