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

import nl.esciencecenter.yarn.YarnSubmitter;

/**
 * Simple example that submits a Constellation application to YARN.
 *
 * This is the starting point of this example. When running this class, it will:
 *
 * - Connect to the local YARN scheduler - Submit a
 * nl.esciencecenter.ApplicationMaster - Wait until the ApplicationMaster has
 * finished.
 */
public class ConstellationSubmitter {

    public static final Logger logger = LoggerFactory
            .getLogger(ConstellationSubmitter.class);

    /**
     * Main entry point of this example that will submit a
     * nl.esciencecenter.ApplicationMaster to YARN.
     *
     * This method expects 4 command line parameters:
     *
     * - The HDFS root directory (for example: /user/jason) - The local
     * directory containing the dependencies (for example: ./dist) - The input
     * file on HDFS to process, assumed to be already present. - The number of
     * workers to use to process the input file (default is 1).
     *
     * @param args
     */
    public static void main(String[] args) {

        String hdfsRoot = args[0];
        String libPath = args[1];
        String inputFile = args[2];
        int workerCount = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        try {
            YarnSubmitter ys = new YarnSubmitter(hdfsRoot, libPath);
            ys.stageIn();
            ys.submitApplicationMaster("nl.esciencecenter.ApplicationMaster",
                    inputFile + " " + workerCount);
            ys.waitForApplication();
            ys.cleanup();
        } catch (Exception e) {
            logger.error("ConstellationOnYarn failed ", e);
        }
    }
}
