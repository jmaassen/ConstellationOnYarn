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

package nl.esciencecenter.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.util.ThreadPool;

/**
 * Simple YARN ApplicationMaster containing the plumbing to starts workers on
 * YARN.
 */
public class YarnMaster {

    public static final Logger logger = LoggerFactory
            .getLogger(YarnMaster.class);

    private String hdfsRoot;
    private String libPath;
    private int containerCount;

    private YarnConfiguration conf;
    private FileSystem fs;

    private Map<String, LocalResource> localResources;

    private AMRMClient<ContainerRequest> rmClient;

    public YarnMaster(String hdfsRoot, String libPath, int containerCount)
            throws IOException {
        this.hdfsRoot = hdfsRoot;
        this.libPath = libPath;
        this.containerCount = containerCount;

        conf = new YarnConfiguration();
        RackResolver.init(conf);
        fs = FileSystem.get(conf);
        localResources = new HashMap<String, LocalResource>();
    }

    /**
     * @return
     */
    public FileSystem getFileSystem() {
        return fs;
    }

    /**
     * Stage in the dependencies in libPath to HDFS.
     *
     * @throws IOException
     */
    public void stageIn() throws IOException {

        logger.info("ApplicationMaster: stage-in dependencies from " + libPath
                + " to " + hdfsRoot + File.separator + libPath);

        // Add all file dependencies that the ApplicationSubmitter has put in
        // HDFS for us to the LocalResources map.
        Utils.addHDFSDirToLocalResources(fs, hdfsRoot, libPath, localResources);
    }

    /**
     * Submit the specified number of containers to YARN.
     *
     * @param jvmOpts
     *            The options to provide to the JVM
     * @param executor
     *            The application to start
     * @param executorOpts
     *            The options of the application
     * @throws YarnException
     * @throws IOException
     */
    public void startWorkers(String jvmOpts, String executor,
            String executorOpts) throws YarnException, IOException {

        logger.info("Starting workers");

        rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        // Create a NodeManager client.
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        RegisterApplicationMasterResponse mresponse = rmClient
                .registerApplicationMaster("", 0, "");

        int maxMem = mresponse.getMaximumResourceCapability().getMemory();
        logger.info(
                "Max mem capabililty of resources in this cluster " + maxMem);

        int maxVCores = mresponse.getMaximumResourceCapability()
                .getVirtualCores();
        logger.info("Max vcores capabililty of resources in this cluster "
                + maxVCores);

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);

        // Ask everything we can, to make sure we get a complete node.
        capability.setMemory(maxMem);
        capability.setVirtualCores(maxVCores);

        // Make container requests to ResourceManager
        for (int i = 0; i < containerCount; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability,
                    /* String [] nodes */null, /* String [] racks */null,
                    priority);

            if (logger.isDebugEnabled()) {
                logger.debug("Doing container request " + i);
            }
            rmClient.addContainerRequest(containerAsk);
        }

        // Setup CLASSPATH for the JVM that will run the ExecutorMain.
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        appMasterEnv.put("CLASSPATH",
                Utils.createClassPath(conf, localResources));
        logger.info("CLASSPATH set to " + appMasterEnv.get("CLASSPATH"));

        // Obtain allocated containers, launch executors and check for responses
        int responseId = 0;
        int launchedContainers = 0;

        LinkedList<AllocateResponse> resp = new LinkedList<>();

        while (launchedContainers < containerCount) {
            AllocateResponse response = rmClient.allocate(responseId++);

            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records
                        .newRecord(ContainerLaunchContext.class);
                Resource c = container.getResource();
                if (logger.isInfoEnabled()) {
                    logger.info(
                            "Container id = " + container.getId().toString());
                    logger.info("VirtualCores = " + c.getVirtualCores());
                    logger.info("Memory = " + c.getMemory());
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Starting container on : "
                            + container.getNodeId().getHost());
                }

                // Try and get a rack identification from the node.
                Node node = RackResolver
                        .resolve(container.getNodeId().getHost());
                node = node.getParent(); // get parent to obtain the rack.

                String rack = "";
                if (node != null) {
                    rack = node.getName();
                }
                List<String> commands = Collections
                        .singletonList(Environment.JAVA_HOME.$$() + "/bin/java"
                                + " -Xmx8192M" + " " + jvmOpts
                                + " -Dlog4j.configuration=file:./dist/log4j.properties"
                                + " -Dibis.constellation.profile=true"
                                + " -Dyarn.constellation.rack=" + rack + " "
                                + executor + " " + executorOpts + " 1>"
                                + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                                + "/executor.stdout" + " 2>"
                                + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                                + "/executor.stderr");

                // System.out.println("CONTAINER STARTS " + commands);

                ctx.setCommands(commands);
                ctx.setEnvironment(appMasterEnv);
                ctx.setLocalResources(localResources);

                // System.out.println("Launching container " + container.getId()
                // + " " + ctx);

                logger.info("Launching worker " + launchedContainers + "/"
                        + containerCount + " " + container.getId());
                logger.info("Commands: " + Arrays.toString(commands.toArray()));

                launchedContainers++;

                nmClient.startContainer(container, ctx);

                resp.add(response);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignored
                }
            }
        }

        final int s = responseId;

        // Create heartbeat thread
        ThreadPool.createNew(new Runnable() {
            @Override
            public void run() {
                for (;;) {
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    try {
                        rmClient.allocate(s);
                    } catch (Throwable e) {
                        // ignore
                    }
                }
            }
        }, "yarnmaster pinger");
    }

    /**
     * Wait for all workers to finish.
     *
     * @throws YarnException
     * @throws IOException
     */
    public void waitForWorkers() throws YarnException, IOException {

        logger.info("Waiting for workers");

        int completedContainers = 0;

        // Wait for the remaining containers to complete
        while (completedContainers != containerCount) {

            AllocateResponse response = rmClient
                    .allocate(completedContainers / containerCount);

            for (ContainerStatus status : response
                    .getCompletedContainersStatuses()) {
                ++completedContainers;
                // System.out.println("ContainerID:" + status.getContainerId() +
                // ", state:" + status.getState().name());
                logger.info("Workers completed " + completedContainers + "/"
                        + containerCount + ", state: "
                        + status.getState().name());
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignored
            }
        }

        logger.info("Stopping YarnMaster");

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                "We rule!!", "");
        rmClient.stop();
    }
}
