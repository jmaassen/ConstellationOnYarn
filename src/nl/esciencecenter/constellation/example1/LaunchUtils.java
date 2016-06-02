/**
 * Copyright 2013 Netherlands eScience Center
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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.util.FSDownload;

/**
 * @version 1.0
 * @since 1.0
 *
 */
public class LaunchUtils {

    public static String createClassPath(YarnConfiguration conf, String appJar, String libPath) { 
        
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(
                ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, 
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {            
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

        // add the runtime classpath needed for tests to work
        //if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
        //    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        //    classPathEnv.append(System.getProperty("java.class.path"));
        //}

        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append(appJar);

        File libdir = new File(libPath);

        File [] libs = libdir.listFiles();
        
        for (File l : libs) { 
            if (l.isFile()) { 
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(libPath + "/" + l.getName());
            }
        }
     
        return classPathEnv.toString();
    }
    /*
    
    public static void createLocalFiles(Configuration conf, String applicationID) { 
               
        LocalDirAllocator localDirAllocator = new LocalDirAllocator(conf.get("hadoop.tmp.dir"));
        FileContext localFSFileContext = FileContext.getLocalFSFileContext();
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        
        Path destPath = localDirAllocator.getLocalPathForWrite(".", conf);
        
        Map<LocalResource, Future<Path>> resourcesToPaths = Maps.newHashMap();
        
        for (LocalResource resource : localResources.values()) {
          Callable<Path> download = new FSDownload(localFSFileContext, ugi, conf, 
                  new Path(destPath, Long.toString(uniqueNumberGenerator.incrementAndGet())),
                  resource);
          
          Path p = download.call();
          // Future<Path> future = exec.submit(download);
          resourcesToPaths.put(resource, future);
        }

        
        
        
    }*/
    
}
