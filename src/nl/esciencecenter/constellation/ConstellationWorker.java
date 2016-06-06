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

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Executor;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitExecutorContext;

/**
 * Simple 'worker' application that creates a Constellation with 1 executor,
 * starts it, then waits for it to finish.
 */
public class ConstellationWorker {

    public static final Logger logger = LoggerFactory
            .getLogger(ConstellationWorker.class);

    public ConstellationWorker() {
        super();
    }

    public static void main(String[] args) {

        logger.info("ConstellationWorker started!");

        try {

            long start = System.currentTimeMillis();

            // This exec should be a command line parameter or property ?
            int exec = 1;

            Executor[] e = new Executor[exec];

            StealStrategy st = StealStrategy.ANY;

            for (int i = 0; i < exec; i++) {
                e[i] = new SimpleExecutor(StealPool.WORLD, StealPool.WORLD,
                        new UnitExecutorContext("test"), st, st, st);
            }

            Properties p = new Properties();
            p.put("ibis.constellation.master", "false");

            Constellation cn = ConstellationFactory.createConstellation(p, e);
            cn.activate();

            long init = System.currentTimeMillis();

            logger.info(
                    "ConstellationWorker init took " + (init - start) + " ms.");

            // Wait for the application to terminate!
            cn.done();

            long end = System.currentTimeMillis();

            logger.info("ConstellationWorker finished after " + (end - start)
                    + " ms.");

        } catch (Exception ex) {
            logger.error("Failed to run Constellation ", ex);
        }
    }
}
