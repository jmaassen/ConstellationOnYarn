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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class Client {
    
    private static final Log LOG = LogFactory.getLog(Client.class);
    
    Configuration conf = new YarnConfiguration();

    public void run(String[] args) throws Exception {
        
        final String command = args[0];
        final int n = Integer.valueOf(args[1]);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        
        
        System.out.println("Connected to YARN");
        //System.out.println(" - Config: " + yarnClient.getConfig());
        System.out.println(" - Queues: " + yarnClient.getAllQueues());
        //System.out.println(" - Applications: " + yarnClient.getApplications());
        System.out.println(" - Labels: " + yarnClient.getClusterNodeLabels());
        
        System.out.println(" - Nodes: " + yarnClient.getNodeReports());
        
        System.out.println(" - Logging to: " + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        
        Set<String> labels = yarnClient.getClusterNodeLabels();
        
        System.out.println(" - Labels: " + labels);
        
        List<NodeReport> nodes = yarnClient.getNodeReports();
        
        for (NodeReport node : nodes) { 
            System.out.println(" - Node: " + node.getRackName() + "/" + node.getNodeId().getHost() + ":" + node.getNodeId().getPort());
        }
        
        
        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        //ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        
        List<String> cmd = Collections.singletonList(
                Environment.JAVA_HOME.$$() + "/bin/java" +
                        " -Xmx256M" +
                        /*" -cp " + "\"/home/jason/Workspace/YarnTest/dist/*:/data/jason/Hadoop/hadoop-2.7.0/share/hadoop/common/*:/data/jason/Hadoop/hadoop-2.7.0/share/hadoop/common/lib/*:/data/jason/Hadoop/hadoop-2.7.0/share/hadoop/yarn/*\"" +*/
                        " test5.ApplicationMaster" +
                        " " + command +
                        " " + String.valueOf(n) +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.stdout" + 
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.stderr" 
                );

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources                 
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        
        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem 
        // Create a local resource to point to the destination jar path 
        
        // FileSystem fs = FileSystem.get(conf);
        
        //addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
        //    localResources, null);

        
        //LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        //setupAppMasterJar(jarPath, appMasterJar);
        
       // amContainer.setLocalResources(Collections.singletonMap("simpleapp.jar", appMasterJar));
        
        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
      
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        classPathEnv.append(':');
        classPathEnv.append("/home/jason/Workspace/YarnTest/dist/simpleapp.jar");
        
        File libdir = new File("/home/jason/Workspace/YarnTest/lib");

        File [] libs = libdir.listFiles();
        
        for (File l : libs) { 
            if (l.isFile()) { 
                classPathEnv.append(':');
                classPathEnv.append(l.getCanonicalPath());
            }
        }
        
        appMasterEnv.put("CLASSPATH", classPathEnv.toString());
        
        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, appMasterEnv, cmd, null, null, null);
        
        // setupAppMasterEnv(appMasterEnv);
        // amContainer.setEnvironment(appMasterEnv);
        
        System.out.println("AM env is " + appMasterEnv);
        

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        appContext.setResource(capability);
        appContext.setApplicationName("simple-yarn-app"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setQueue("default"); // default queue 
        
        Priority pri = Priority.newInstance(0);
        appContext.setPriority(pri);
        
        System.out.println("AppContext is " + appContext);
        
        // Submit application
        yarnClient.submitApplication(appContext);
        
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && 
                appState != YarnApplicationState.KILLED && 
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println("Application " + appId + " finished with" + " state " + appState + " at " + appReport.getFinishTime());
    }
    
//    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
// 
//        System.out.println("My AppMasterJar in HDFS is " + jarPath);
//        
//        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
//
//        System.out.println("  stat is " + jarStat);
//        
//        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
//        appMasterJar.setSize(jarStat.getLen());
//        appMasterJar.setTimestamp(jarStat.getModificationTime());
//        appMasterJar.setType(LocalResourceType.FILE);
//        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
//        
//        System.out.println("  Resulting localResource is " + appMasterJar);        
//    }
//
//    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
//        for (String c : conf.getStrings(
//                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
//                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
//            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),c.trim());
//        }
//        Apps.addToEnvironment(appMasterEnv,
//                Environment.CLASSPATH.name(),
//                Environment.PWD.$() + File.separator + "*");
//    }

    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.run(args);
    }
}
