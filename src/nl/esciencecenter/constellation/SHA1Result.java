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

package nl.esciencecenter.constellation;

import java.io.Serializable;

/**
 * Container for the result of a SHA1Job
 */
public class SHA1Result implements Serializable {

    // Generated
    private static final long serialVersionUID = -1475352090350923307L;

    private final String file;
    private final int block;
    private final byte[] SHA1;
    private final long time;
    private final Throwable e;

    private SHA1Result(String file, int block, byte[] sha1, long time,
            Throwable e) {
        this.file = file;
        this.block = block;
        this.SHA1 = sha1;
        this.time = time;
        this.e = e;

    }

    public SHA1Result(String file, int block, byte[] sha1, long time) {
        this(file, block, sha1, time, null);
    }

    public SHA1Result(String file, int block, Throwable e) {
        this(file, block, null, 0, e);
    }

    public String getFile() {
        return file;
    }

    public int getBlock() {
        return block;
    }

    public byte[] getSHA1() {
        return SHA1;
    }

    public Throwable getException() {
        return e;
    }

    public boolean hasFailed() {
        return e != null;
    }

    public long getTime() {
        return time;
    }
}
