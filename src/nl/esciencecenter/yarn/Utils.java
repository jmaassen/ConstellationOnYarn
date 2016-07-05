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

package nl.esciencecenter.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of utilities to copy files to HDFS and generate a LocalResources map and
 * CLASSPATH.
 */
public class Utils {

    public static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Copies all files in a local file system directory to HDFS using hdfsRoot
     * as the root.
     *
     * The directory itself is also created on HDFS. The copy is not recursive.
     *
     * For example, when copying './lib' containing
     *
     * ./lib/test.jar ./lib/data.tgz ./lib/dir/test2.jar
     *
     * to hdfsRoot '/user/test', the following directory and files on HDFS will
     * be the result:
     *
     * /user/test/lib/test.jar /user/test/lib/data.tgz
     *
     * In addition, each file will optionally be added to the localResources
     * map, using 'directory/filename' as a key.
     *
     * @param fs
     *            The (HDFS) FileSystem to use
     * @param directory
     *            The local directory
     * @param hdfsRoot
     *            The root in HDFS to copy to
     * @param localResources
     *            The (optional) localResourcesMap to add each file to.
     * @throws IOException
     */
    public static void copyLocalDirToHDFS(FileSystem fs, String directory,
            String hdfsRoot, Map<String, LocalResource> localResources)
            throws IOException {

        File libdir = new File(directory);

        File[] libs = libdir.listFiles();

        for (File l : libs) {
            if (l.isFile()) {
                copyLocalFileToHDFS(fs,
                        directory + File.separator + l.getName(), hdfsRoot,
                        localResources);
            }
        }
    }

    /**
     * Copies a local file on a local file system directory to HDFS using
     * hdfsRoot as the root. Any parent directories of the file will also be
     * copied.
     *
     * For example, when copying './lib/test.jar to hdfsRoot '/user/test', the
     * result on HDFS will be '/user/test/lib/test.jar'.
     *
     * In addition, the file will optionally be added to the localResources map,
     * using 'directory/filename' as a key.
     *
     * @param fs
     *            The (HDFS) FileSystem to use
     * @param file
     *            The local file
     * @param hdfsRoot
     *            The root in HDFS to copy to
     * @param localResources
     *            The (optional) localResourcesMap to add each file to.
     * @throws IOException
     */
    public static void copyLocalFileToHDFS(FileSystem fs, String file,
            String hdfsRoot, Map<String, LocalResource> localResources)
            throws IOException {

        Path localPath = new Path(file);
        Path hdfsPath = new Path(hdfsRoot, file);

        if (logger.isInfoEnabled()) {
            logger.info("Copying " + localPath + " to " + hdfsPath
                    + " and adding to localResources");
        }

        fs.copyFromLocalFile(false, true, localPath, hdfsPath);

        if (localResources != null) {
            addHDFSFileToLocalResources(fs, hdfsRoot, hdfsPath, localResources);
        }
    }

    /**
     * Add a single file on HDFS to the localResources map.
     *
     * The hdfsRoot specifies the root on the HDFS file system. This part of the
     * part will be removed from the key in the localResources. For example, if
     * the following HDFS file '/user/test/lib/test.jar' is added with HDFS root
     * '/user/test' the string 'lib/test.jar' will be used as key in
     * localResources.
     *
     * @param fs
     *            The (HDFS) FileSystem to use
     * @param hdfsRoot
     *            The root in HDFS
     * @param hdfsPath
     *            The path of the file in HDFS
     * @param localResources
     *            The localResource to add the file to
     * @throws IOException
     */
    public static void addHDFSFileToLocalResources(FileSystem fs,
            String hdfsRoot, Path hdfsPath,
            Map<String, LocalResource> localResources) throws IOException {

        FileStatus s = fs.getFileStatus(hdfsPath);
        URL packageUrl = ConverterUtils.getYarnUrlFromPath(
                FileContext.getFileContext().makeQualified(hdfsPath));
        LocalResource r = LocalResource.newInstance(packageUrl,
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                s.getLen(), s.getModificationTime());

        String subPath = hdfsPath.toUri().getPath()
                .substring(hdfsRoot.length());

        if (logger.isDebugEnabled()) {
            logger.debug("Adding to localResources " + subPath + " " + r);
        }

        localResources.put(subPath, r);
    }

    /**
     * Add each file in the specified directory on HDFS to localResources. Any
     * subdirectories will be skipped.
     *
     * The directory should be specified relative to the hdfsRoot. For example,
     * if the hdfsRoot is set to '/user/test' and the directory is 'lib', the
     * files in the resulting path '/user/test/lib' will be added.
     *
     * @param fs
     *            The (HDFS) FileSystem to use
     * @param hdfsRoot
     *            The root in HDFS
     * @param dir
     *            The directory in HDFS to add, relative to hdfsRoot
     * @param localResources
     *            The localResource to add the file to
     * @throws IOException
     */
    public static void addHDFSDirToLocalResources(FileSystem fs,
            String hdfsRoot, String dir,
            Map<String, LocalResource> localResources) throws IOException {

        if (logger.isInfoEnabled()) {
            logger.info("Adding dir " + dir + " to local resources");
        }

        Path p = new Path(hdfsRoot, dir);

        if (!fs.exists(p)) {
            logger.warn("Cannot add dir " + dir + " (not found)");
            return;
        }

        if (!fs.isDirectory(p)) {
            logger.warn("Cannot add dir " + dir + " (not a dir)");
            return;
        }

        FileStatus[] list = fs.listStatus(p);

        if (list == null || list.length == 0) {
            logger.warn("Cannot add dir " + dir + " (dir empty)");
            return;
        }

        for (int i = 0; i < list.length; i++) {
            FileStatus s = list[i];

            if (!s.isDirectory()) {
                addHDFSFileToLocalResources(fs, hdfsRoot, s.getPath(),
                        localResources);
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Skipping subdir " + s.getPath());
                }
            }
        }
    }

    /**
     * Generate a CLASSPATH for a JVM using the setup described in 'conf' and
     * the files in 'localResources'.
     *
     * @param conf
     *            The Configuration describing the local YARN setup.
     * @param localResources
     *            The LocalResources describing the set of files that needs to
     *            be localized to run the JVM.
     * @return
     */
    public static String createClassPath(YarnConfiguration conf,
            Map<String, LocalResource> localResources) {

        StringBuilder classPathEnv = new StringBuilder(
                Environment.CLASSPATH.$$())
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("./*");

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                .append("./log4j.properties");

        // Add add localized files to classpath
        for (Entry<String, LocalResource> e : localResources.entrySet()) {

            LocalResource r = e.getValue();

            if (r.getType() == LocalResourceType.FILE && r
                    .getVisibility() == LocalResourceVisibility.APPLICATION) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(e.getKey());
            }
        }

        return classPathEnv.toString();
    }

    public static void removeHDFSDir(FileSystem fs, String dir, String hdfsRoot)
            throws IOException {
        Path p = new Path(hdfsRoot, dir);

        if (!fs.exists(p)) {
            logger.warn("Cannot remove dir " + dir + " (not found)");
            return;
        }

        if (!fs.isDirectory(p)) {
            logger.warn("Cannot remove dir " + dir + " (not a dir)");
            return;
        }

        fs.delete(p, true);
    }
}
