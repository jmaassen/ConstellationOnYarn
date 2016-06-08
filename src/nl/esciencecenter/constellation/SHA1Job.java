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

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.ActivityContext;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Event;
import ibis.constellation.SimpleActivity;

/**
 * Simple test job that computes a SHA1 hash of a single block in an input file.
 */
public class SHA1Job extends SimpleActivity {

    public static final Logger logger = LoggerFactory.getLogger(SHA1Job.class);

    private static final long serialVersionUID = -5546760613223653596L;

    private final static int BUFFERSIZE = 64 * 1024;

    private final String file;
    private final int blockIndex;
    private final long offset;
    private final long length;

    public SHA1Job(ActivityIdentifier parent, ActivityContext context,
            String file, int blockIndex, long offset, long length) {
        super(parent, context);
        this.file = file;
        this.blockIndex = blockIndex;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public void simpleActivity() {

        logger.info("Running SHA1Job " + file + " " + blockIndex + " " + offset
                + " " + length);
        logger.info(
                "Executor context = " + getExecutor().getContext().toString());
        logger.info("Activity context = " + getContext().toString());

        // Create a buffer for the input data
        byte[] buffer = new byte[BUFFERSIZE];

        long start = System.currentTimeMillis();

        FileSystem fs = null;

        try {
            // Open the input file.
            fs = FileSystem.newInstance(new Configuration());
            Path inputfile = new Path(file);

            // Create the SHA1 digest
            MessageDigest m = MessageDigest.getInstance("SHA1");

            if (!fs.exists(inputfile)) {
                throw new Exception("Could not find input file!");
            }

            FSDataInputStream in = fs.open(inputfile);
            in.seek(offset);

            // Read the file and compute the SHA1 of this block
            long pos = offset;

            while (pos < offset + length) {
                int len = (int) Math.min(length - (pos - offset), BUFFERSIZE);
                in.readFully(buffer, 0, len);
                m.update(buffer, 0, len);
                pos += len;
            }

            byte[] digest = m.digest();

            long end = System.currentTimeMillis();

            logger.info("SHA1Job " + file + " " + blockIndex + " " + offset
                    + " " + length + " successful and took " + (end - start)
                    + " ms");

            getExecutor().send(new Event(identifier(), getParent(),
                    new SHA1Result(file, blockIndex, digest)));

        } catch (Throwable e) {
            logger.error("SHA1Job " + file + " " + blockIndex + " " + offset
                    + " " + length + " failed.", e);

            getExecutor().send(new Event(identifier(), getParent(),
                    new SHA1Result(file, blockIndex, e)));
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                // ignore?
            }
        }

    }
}
