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
     * This method expects 4 or 5 command line parameters: <br>
     * - The HDFS root directory (for example: /user/jason) <br>
     * - The local directory containing the dependencies (for example: ./dist)
     * <br>
     * - The input file on HDFS to process, assumed to be already present <br>
     * - flag indicating whether to use specific contexts or not (true or false)
     * <br>
     * - Optionally, he number of workers to use to process the input file
     * (default is 1).
     *
     * @param args
     */
    public static void main(String[] args) {

        String hdfsRoot = args[0];
        String libPath = args[1];
        String inputFile = args[2];
        String flag = args[3];
        int workerCount = args.length > 4 ? Integer.parseInt(args[4]) : 1;
        String execCount = args.length > 5 ? args[5] : "";

        try {
            YarnSubmitter ys = new YarnSubmitter(hdfsRoot, libPath);
            ys.stageIn();
            logger.info("HDFS root = " + hdfsRoot);
            logger.info("Inputfile = " + inputFile);
            logger.info("Use specific contexts = " + flag);
            logger.info("WorkerCount = " + workerCount);
            logger.info("execCount = " + ("".equals(execCount) ? "default" : execCount));
            ys.submitApplicationMaster("nl.esciencecenter.ApplicationMaster",
                    inputFile + " " + flag + " " + workerCount + " "
                            + execCount, workerCount);
            ys.waitForApplication();
            ys.cleanup();
        } catch (Exception e) {
            logger.error("ConstellationOnYarn failed ", e);
        }
    }
}
