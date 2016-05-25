package nl.esciencecenter.constellation.example1;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.Set;





import org.apache.commons.io.IOUtils;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
//import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationSubmitter {

    //    private static final Log LOG = LogFactory.getLog(ApplicationSubmitter.class);

    Configuration conf = new YarnConfiguration();

    public void run(String[] args) throws Exception {

        System.out.println("APPLICATION SUBMITTER " + Arrays.toString(args));
        
        String inputFile = args[0];
        String mainClass = args[1];
        String appJar = args[2];
        String libPath = args[3];
        int n = Integer.parseInt(args[4]);    

        // Create yarnClient
        YarnClient yarnClient = YarnClient.createYarnClient();
        YarnConfiguration conf = new YarnConfiguration();
        yarnClient.init(conf);
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        System.out.println("Got Cluster metric info from ASM, numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);

        System.out.println("Got Cluster node info from ASM");

        for (NodeReport node : clusterNodeReports) {
            System.out.println("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo("default");
        System.out.println("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                System.out.println("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        
        ApplicationId id = appResponse.getApplicationId();

        System.out.println("Connected to YARN with application ID: " + id);

        // Not all of this works on older hadoop installations (like on DAS4)

        // System.out.println(" - Config: " + yarnClient.getConfig());
        // System.out.println(" - Queues: " + yarnClient.getAllQueues());
        // System.out.println(" - Applications: " + yarnClient.getApplications());
        // System.out.println(" - Labels: " + yarnClient.getClusterNodeLabels());

        // System.out.println(" - Nodes: " + yarnClient.getNodeReports());

        // System.out.println(" - Logging to: " + ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        // Not supported on hadoop 2.5.0 ? 
        // Set<String> labels = yarnClient.getClusterNodeLabels();
        //
        // System.out.println(" - Labels: " + labels);

        // List<NodeReport> nodes = yarnClient.getNodeReports();

        // for (NodeReport node : nodes) { 
        //    System.out.println(" - Node: " + node.getRackName() + "/" + node.getNodeId().getHost() + ":" + node.getNodeId().getPort());
        // }

        // Set up the container launch context for the application master
        // ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        List<String> cmd = Collections.singletonList(
                Environment.JAVA_HOME.$$() + "/bin/java" +
                        " -Xmx256M" +
                        " " + mainClass + 
                        " " + inputFile +
                        " " + appJar + 
                        " " + libPath + 
                        " " + String.valueOf(n) +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.stdout" + 
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.stderr" 
                );

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources                 
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        FileSystem fs = FileSystem.get(conf);
        
        addLocalJar(fs, appJar, localResources);
        
        System.out.println("Adding " + libPath + " to local resources");
        
        File libdir = new File(libPath);

        File [] libs = libdir.listFiles();

        for (File l : libs) { 
            if (l.isFile()) { 
                addLocalJar(fs, libdir + "/" + l.getName(), localResources);                      
            }
        }
        
        //addToLocalResources(fs, appJar, appJar, id.toString(), localResources, null);

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
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append(appJar);

        for (File l : libs) { 
            if (l.isFile()) { 
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(libPath + "/" + l.getName());
            }
        }

        appMasterEnv.put("CLASSPATH", classPathEnv.toString());

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, appMasterEnv, cmd, null, null, null);

        System.out.println("ApplicationMaster environment is " + appMasterEnv);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        appContext.setResource(capability);
        appContext.setApplicationName("ConstellationOnYarn-example1"); // application name
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
    
    private void addLocalJar(FileSystem fs, String file, Map<String, LocalResource> localResources) throws IOException { 
     
        File packageFile = new File(file);

        Path p = new Path(file);
        
        System.out.println("Adding " + file + " to local resources");
        
        fs.copyFromLocalFile(false, true, p, p);
        FileStatus s = fs.getFileStatus(p);
        
        URL packageUrl = ConverterUtils.getYarnUrlFromPath(FileContext.getFileContext().makeQualified(p));
        LocalResource r = LocalResource.newInstance(packageUrl, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, s.getLen(), s.getModificationTime());        
        localResources.put(file, r);
    }    
    
/*
    private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId, Map<String, LocalResource> localResources, String resources) throws IOException {

//        String suffix = "ConstellationOnYarn" + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), fileDstPath);
        
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(false, true, new Path(fileSrcPath), dst);
        }
        
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        
        localResources.put(fileDstPath, scRsrc);
    }
*/
    
    public static void main(String[] args) throws Exception {
        ApplicationSubmitter c = new ApplicationSubmitter();
        c.run(args);
    }
}
