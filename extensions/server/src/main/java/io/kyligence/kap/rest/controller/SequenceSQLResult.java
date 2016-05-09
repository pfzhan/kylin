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

package io.kyligence.kap.rest.controller;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.kylin.rest.response.SQLResponse;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SequenceSQLResult implements Serializable{
    List<List<String>> results;

    public SequenceSQLResult(SQLResponse sqlResponse) {
        this.results = sqlResponse.getResults();
    }

    public void intersect(SequenceSQLResult other) {
        Set<List<String>> otherInSet = Sets.newHashSet(other.results);
        results.retainAll(otherInSet);
    }

    public void union(SequenceSQLResult other) {
        Set<List<String>> union = Sets.newHashSet();
        for (List<String> row : this.results) {
            union.add(row);
        }
        for (List<String> row : other.results) {
            union.add(row);
        }
        this.results = Lists.newArrayList(union);
    }

    public void forwardExcept(SequenceSQLResult other) {
        Set<List<String>> remaining = Sets.newHashSet(this.results);
        for (List<String> row : other.results) {
            remaining.remove(row);
        }
        this.results = Lists.newArrayList(remaining);
    }

    public void backwardExcept(SequenceSQLResult other) {
        Set<List<String>> remaining = Sets.newHashSet(other.results);
        for (List<String> row : this.results) {
            remaining.remove(row);
        }
        this.results = Lists.newArrayList(remaining);
    }
    
    public int size()
    {
        return results.size();
    }

}
