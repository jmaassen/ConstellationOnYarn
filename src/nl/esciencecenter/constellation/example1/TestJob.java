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

import ibis.constellation.ActivityContext;
import ibis.constellation.ActivityIdentifier;
import ibis.constellation.Event;
import ibis.constellation.SimpleActivity;

/**
 * Simple test job that prints the job assigned to it, sleeps for 2 minutes, and sends an event to its parent to signal that it
 * has finished.  
 */
public class TestJob extends SimpleActivity {

    private static final long serialVersionUID = -5546760613223653596L;

    private final String file;
    private final int blockIndex;
    
    public TestJob(ActivityIdentifier parent, ActivityContext context, String file, int blockIndex) {
        super(parent, context);
        this.file = file;
        this.blockIndex = blockIndex;
    }

    @Override
    public void simpleActivity() throws Exception {
        
        System.out.println("TestJob " + file + " / " + blockIndex);
        
        try { 
            Thread.sleep(120*1000);
        } catch (Exception e) { 
            // ignore
        }
        
        getExecutor().send(new Event(identifier(), getParent(), file + "/" + blockIndex));
    }
}
