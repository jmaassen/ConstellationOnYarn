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

package nl.esciencecenter.constellation;

import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Event;
import ibis.constellation.Executor;
import ibis.constellation.MultiEventCollector;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitActivityContext;
import ibis.constellation.context.UnitExecutorContext;
import ibis.ipl.server.Server;
import ibis.util.TypedProperties;

/**
 * Master application for this example.
 *
 * Initializes a IPLServer and creates a local Constellation. Next divides the
 * input file into blocks and submits a SHA1Job for each block. Then waits for
 * the results to come in.
 */
public class ConstellationMaster {

    public static final Logger logger = LoggerFactory
            .getLogger(ConstellationMaster.class);

    private Server server;

    private Constellation cn;
    private MultiEventCollector sec;
    private ActivityIdentifier secid;

    private long start;
    private long end;

    private FileSystem fs;

    private String address;

    public ConstellationMaster(FileSystem fs) {
        this.fs = fs;
    }

    // Start a local Constellation using a single executor only used to gather
    // the results.
    private final void startConstellation(String address) throws Exception {

        logger.info("Starting Constellation");

        start = System.currentTimeMillis();

        int exec = 1;

        Executor[] e = new Executor[exec];

        StealStrategy st = StealStrategy.ANY;

        for (int i = 0; i < exec; i++) {
            e[i] = new SimpleExecutor(StealPool.WORLD, StealPool.WORLD,
                    new UnitExecutorContext("master"), st, st, st);
        }

        Properties p = new Properties();
        p.put("ibis.constellation.master", "true");
        p.put("ibis.pool.name", "test");
        p.put("ibis.server.address", address);

        cn = ConstellationFactory.createConstellation(p, e);
        cn.activate();

        long init = System.currentTimeMillis();

        logger.info("Constellation test init took: " + (init - start) + " ms.");
    }

    /**
     * Initialize the IPL and Constellation on this process.
     *
     * @throws Exception
     */
    public void initialize() throws Exception {

        // Start an Ibis server here, to serve the pool of constellations.
        TypedProperties properties = new TypedProperties();
        properties.putAll(System.getProperties());

        server = new Server(properties);
        address = server.getAddress();

        logger.info("Started server at: " + address);

        // Start a Constellation here that only serves as a source of jobs and
        // sink of results.

        logger.info("Starting Constellation");

        startConstellation(address);
    }

    /**
     * Return the JVM options needed by the worker to reach the IPL server.
     *
     * @return the JVM options needed by the worker to reach the IPL server.
     */
    public String getJVMOpts() {
        return " -Dibis.pool.name=test" + " -Dibis.server.address=" + address;
    }

    /**
     * Splits the provided HDFS input file into blocks and submit a SHA1Job for
     * each block.
     *
     * @param inputFile
     *            The HDFS input file
     * @throws Exception
     */
    public void submitJobs(String inputFile) {

        // Find the test input file.

        Path testfile = new Path(inputFile);

        FileStatus stat = null;
        BlockLocation[] locs = null;

        try {
            stat = fs.getFileStatus(testfile);
            if (logger.isInfoEnabled()) {
                logger.info("Found input file " + testfile.getName()
                        + " with length " + stat.getLen() + " blocksize "
                        + stat.getBlockSize() + " replication "
                        + stat.getReplication());
            }
        } catch (Throwable e) {
            logger.error("could not get status of file " + testfile, e);
        }

        if (stat != null) {
            try {
                locs = fs.getFileBlockLocations(testfile, 0, stat.getLen());
            } catch (Throwable e) {
                logger.error(
                        "could not get block locations of file " + testfile, e);
            }
        }

        // Sumbit collector job here to collect replies
        logger.info("Submitting event collector");

        sec = new MultiEventCollector(new UnitActivityContext("master"),
                locs == null ? 0 : locs.length);
        secid = cn.submit(sec);

        // Generate a Job for each block
        if (locs != null) {
            logger.info("Block locations: ");

            for (int i = 0; i < locs.length; i++) {
                BlockLocation b = locs[i];

                if (logger.isInfoEnabled()) {
                    try {
                        logger.info("Block " + b.getOffset() + " - "
                                + (b.getOffset() + b.getLength())
                                + ", Block locations: "
                                + Arrays.toString(b.getHosts()));
                        logger.info("Cached locations: "
                                + Arrays.toString(b.getCachedHosts())
                                + "Names: " + Arrays.toString(b.getNames()));
                        logger.info("Topo paths: "
                                + Arrays.toString(b.getTopologyPaths()));
                    } catch (Throwable e) {
                        logger.error("Got exception in verbose", e);
                    }
                    logger.info("Submitting TestJob " + i);
                }

                SHA1Job job = new SHA1Job(secid,
                        new UnitActivityContext("test"), inputFile, i,
                        b.getOffset(), b.getLength());

                cn.submit(job);
            }
        }
    }

    // Convert a SHA1 hash to a String so we can print it.
    private final String SHA1toString(byte[] sha1) {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < sha1.length; i++) {
            sb.append(Integer.toString((sha1[i] & 0xff) + 0x100, 16)
                    .substring(1));
        }

        return sb.toString();
    }

    /**
     * Wait for all jobs to return a result.
     *
     * @throws Exception
     */
    public void waitForJobs() throws Exception {

        Event[] events = sec.waitForEvents();

        System.out.println("Results: ");

        for (Event e : events) {

            SHA1Result result = (SHA1Result) e.data;

            if (result.hasFailed()) {
                System.out.println("  " + result.getBlock() + " FAILED");
            } else {
                System.out.println("  " + result.getBlock() + " "
                        + SHA1toString(result.getSHA1()));
            }
        }

        end = System.currentTimeMillis();

        System.out.println(
                "Constellation test run took: " + (end - start) + " ms.");
    }

    /**
     * Stop Constellation and the IPL server.
     */
    public void cleanup() {

        try {
            cn.done();
        } catch (Throwable e) {
            logger.error("Failed to terminate Constellation!", e);
        }

        try {
            // Kill the ibis server
            server.end(10 * 1000);
        } catch (Throwable e) {
            logger.error("Failed to terminate IPL Server!", e);
        }
    }

}
