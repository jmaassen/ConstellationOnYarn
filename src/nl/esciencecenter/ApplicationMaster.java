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

package nl.esciencecenter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.esciencecenter.constellation.ConstellationMaster;
import nl.esciencecenter.yarn.YarnMaster;

/**
 * Simple Application master that combines a YarnMaster and ConstellationMaster
 * into a single master application.
 */
public class ApplicationMaster {

    public static final Logger logger = LoggerFactory
            .getLogger(ApplicationMaster.class);

    /**
     * Do not run directly!! This application should be submitted to be run
     * inside a YARN cluster.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // Input parameters here
        String hdfsRoot = args[0];
        String libPath = args[1];
        String inputFile = args[2];
        String flag = args[3];
        int containerCount = Integer.parseInt(args[4]);

        try {
            YarnMaster m = new YarnMaster(hdfsRoot, libPath, containerCount);
            m.stageIn();

            ConstellationMaster c = new ConstellationMaster(m.getFileSystem());
            c.initialize();

            m.startWorkers(c.getJVMOpts(),
                    "nl.esciencecenter.constellation.ConstellationWorker", "");

            c.submitJobs(inputFile, flag);
            c.waitForJobs();
            c.cleanup();

            m.waitForWorkers();

        } catch (Exception e) {
            logger.error("ApplicationMaster Failed", e);
        }
    }
}
