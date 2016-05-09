/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.request;

import org.apache.kylin.rest.request.SQLRequest;

public class SequenceSQLRequest extends SQLRequest {

    public enum OptWithLastResult {
        NONE, //for the first query in the sequence
        INTERSECT, //
        UNION, //
        FORWARD_EXCEPT, // current result - last result
        BACKWARD_EXCEPT// last result - current result
    }

    protected long sessionID;
    protected OptWithLastResult opt;

    public long getSessionID() {
        return sessionID;
    }

    public void setSessionID(long sessionID) {
        this.sessionID = sessionID;
    }

    public OptWithLastResult getOpt() {
        return opt == null ? OptWithLastResult.NONE : opt;
    }

    public void setOpt(OptWithLastResult opt) {
        this.opt = opt;
    }
}
