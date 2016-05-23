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

package nl.esciencecenter.constellation.example1;

import ibis.constellation.Constellation;
import ibis.constellation.ConstellationFactory;
import ibis.constellation.Executor;
import ibis.constellation.SimpleExecutor;
import ibis.constellation.StealPool;
import ibis.constellation.StealStrategy;
import ibis.constellation.context.UnitExecutorContext;

import java.util.Properties;

/**
 * Simple 'worker' application that creates a Constellation with 1 executor, starts it, then waits for it to finish.  
 */
public class ExecutorMain {

    public ExecutorMain() {
        super();
    }

    public static void main(String [] args) { 
        
        System.out.println("ExecutorMain started!");     
        
        try {
            
            long start = System.currentTimeMillis();

            // This exec should be a command line parameter or property ? 
            int exec = 1;
            
            Executor [] e = new Executor[exec];
            
            StealStrategy st = StealStrategy.ANY;
            
            for (int i=0;i<exec;i++) { 
                e[i] = new SimpleExecutor(StealPool.WORLD, StealPool.WORLD, new UnitExecutorContext("test"), st, st, st);
            }

            Properties p = new Properties();
            p.put("ibis.constellation.master", "false");
            
            Constellation cn = ConstellationFactory.createConstellation(p, e);
            cn.activate();
        
            long init = System.currentTimeMillis();

            System.out.println("Constellation test init took: " + (init-start) + " ms.");

            // Wait for the application to terminate!
            cn.done();

            long end = System.currentTimeMillis();

            System.out.println("Constellation test run took: " + (end-start) + " ms.");

        } catch (Exception ex) { 
            System.err.println("Failed to run Constellation " + ex);
            ex.printStackTrace(System.err);            
        }
    }
}
