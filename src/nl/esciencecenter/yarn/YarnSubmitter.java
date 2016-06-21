package nl.esciencecenter.yarn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple YARN client application that connects to a YARN scheduler.
 */
public class YarnSubmitter {

    public static final Logger logger = LoggerFactory
            .getLogger(YarnSubmitter.class);

    private String hdfsRoot;
    private String libPath;

    private YarnClient yarnClient;
    private YarnConfiguration conf;

    private FileSystem fs;
    private Map<String, LocalResource> localResources;

    private ApplicationId appId;

    public YarnSubmitter(String hdfsRoot, String libPath) throws IOException {
        this.hdfsRoot = hdfsRoot;
        this.libPath = libPath;

        yarnClient = YarnClient.createYarnClient();
        conf = new YarnConfiguration();
        yarnClient.init(conf);
        yarnClient.start();

        fs = FileSystem.get(conf);
    }

    public void stageIn() throws IOException {
        localResources = new HashMap<String, LocalResource>();
        Utils.copyLocalDirToHDFS(fs, libPath, hdfsRoot, localResources);
    }

    public void submitApplicationMaster(String mainClass,
            String applicationOptions) throws YarnException, IOException {

        // Create a new application in YARN
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        ApplicationId id = appResponse.getApplicationId();

        logger.info("Connected to YARN with application ID: " + id);

        // Create the CLASSPATH for ApplicationMaster and include the file
        // dependencies in the LocalResources map.
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        appMasterEnv.put("CLASSPATH",
                Utils.createClassPath(conf, localResources));

        // Set up the container launch context for the application master
        List<String> cmd = Collections.singletonList(Environment.JAVA_HOME.$$()
                + "/bin/java" + " -Xmx64M"
                + " -Dlog4j.configuration=file:./dist/log4j.properties" + " "
                + mainClass + " " + hdfsRoot + " " + libPath + " "
                + applicationOptions + " 1>"
                + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/master.stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                + "/master.stderr");

        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, appMasterEnv, cmd, null, null, null);

        // System.out.println("ApplicationMaster environment is " +
        // appMasterEnv);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app
                .getApplicationSubmissionContext();
        appId = appContext.getApplicationId();

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(64);
        capability.setVirtualCores(1);
        appContext.setResource(capability);
        appContext.setApplicationName("ConstellationOnYarn"); // application
                                                              // name
        appContext.setAMContainerSpec(amContainer);
        appContext.setQueue("default"); // default queue

        Priority pri = Priority.newInstance(0);
        appContext.setPriority(pri);

        // Submit application. Keep this as a println, so that we always see the
        // application id.
        System.out.println(
                "Submitting ApplicationMaster to YARN with ID " + appId);
        yarnClient.submitApplication(appContext);
    }

    public void waitForApplication() throws YarnException, IOException {

        logger.info("Waiting until ApplicationMaster is finished...");

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();

        while (appState != YarnApplicationState.FINISHED
                && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignored
            }

            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        logger.info("Application " + appId + " finished with" + " state "
                + appState + " at " + appReport.getFinishTime());
    }

    public void cleanup() throws IOException {
        yarnClient.close();
    }
}
