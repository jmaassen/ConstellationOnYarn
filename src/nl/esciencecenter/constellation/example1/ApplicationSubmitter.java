package nl.esciencecenter.constellation.example1;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
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

public class ApplicationSubmitter {
    
//    private static final Log LOG = LogFactory.getLog(ApplicationSubmitter.class);
    
    Configuration conf = new YarnConfiguration();

    public void run(String[] args) throws Exception {

        String inputFile = args[0];
        String mainClass = args[1];
        String appJar = args[2];
        String libPath = args[3];
        int n = Integer.parseInt(args[4]);    
        
        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        
        System.out.println("Connected to YARN");
        //System.out.println(" - Config: " + yarnClient.getConfig());
        System.out.println(" - Queues: " + yarnClient.getAllQueues());
        // System.out.println(" - Applications: " + yarnClient.getApplications());
        // System.out.println(" - Labels: " + yarnClient.getClusterNodeLabels());
        
        System.out.println(" - Nodes: " + yarnClient.getNodeReports());
        
        System.out.println(" - Logging to: " + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        
        // Not supported on hadoop 2.5.0 ? 
        // Set<String> labels = yarnClient.getClusterNodeLabels();
        //
        // System.out.println(" - Labels: " + labels);
        
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
        classPathEnv.append(appJar);
        
        File libdir = new File(libPath);

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

    public static void main(String[] args) throws Exception {
        ApplicationSubmitter c = new ApplicationSubmitter();
        c.run(args);
    }
}
