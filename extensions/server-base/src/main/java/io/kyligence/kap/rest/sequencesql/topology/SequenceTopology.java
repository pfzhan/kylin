/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.rest.sequencesql.topology;

import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.sequencesql.DiskResultCache;
import io.kyligence.kap.rest.sequencesql.ResultOpt;
import io.kyligence.kap.rest.sequencesql.SequenceNodeOutput;
import io.kyligence.kap.rest.sequencesql.SequenceOpt;

public class SequenceTopology {

    private static final Logger logger = LoggerFactory.getLogger(SequenceTopology.class);

    private SequenceSQLNode first = null;//must be a SequenceSQLNode
    private int sqlNodeCount = 0;
    private int optNodeCount = 0;
    private DiskResultCache diskResultCache;
    private long sequenceID;
    private int workerID;

    public SequenceTopology(int workerID, DiskResultCache diskResultCache, long sequenceID) {
        this.workerID = workerID;
        this.diskResultCache = diskResultCache;
        this.sequenceID = sequenceID;
    }

    // return stepID
    public int addStep(String sql, SequenceOpt sequenceOpt, ResultOpt resultOpt) {
        SequenceNode lowest = findLowestChild();
        if (lowest == null) {
            if (sequenceOpt != SequenceOpt.INIT) {
                throw new IllegalStateException("Current topology has no nodes, meeting invalid opt:" + sequenceOpt);
            }
            SequenceSQLNode sequenceSQLNode = new SequenceSQLNode(sql, sqlNodeCount++);
            first = sequenceSQLNode;
            return getStepIDFromSqlID(sequenceSQLNode.sqlID);
        } else {
            if (sequenceOpt == SequenceOpt.INIT) {
                throw new IllegalStateException("SequenceOpt.INIT is only allowed for the first sql in the sequence");
            }
            SequenceSQLNode newSQLNode = new SequenceSQLNode(sql, sqlNodeCount++);
            SequenceOptNode newOptNode = new SequenceOptNode(Lists.newArrayList(lowest, newSQLNode), resultOpt, optNodeCount++);
            lowest.child = newOptNode;
            newSQLNode.child = newOptNode;
            return getStepIDFromSqlID(newSQLNode.sqlID);
        }
    }

    public int updateStep(int stepID, String sql, SequenceOpt sequenceOpt, ResultOpt resultOpt) {
        if (sequenceOpt != SequenceOpt.UPDATE) {
            throw new IllegalStateException();
        }

        SequenceSQLNode sqlNode = findSQLNode(getSqlIDFromStepID(stepID));
        if (sql != null) {
            sqlNode.setSql(sql);
        }

        SequenceOptNode optNode = (SequenceOptNode) sqlNode.child;
        if (resultOpt != null) {
            optNode.setOpt(resultOpt);
        }
        return getStepIDFromSqlID(sqlNode.getSqlID());
    }

    public int getSqlIDFromStepID(int stepID) {
        return stepID;
    }

    public int getStepIDFromSqlID(int sqlID) {
        return sqlID;
    }

    public int updateSQLNodeResult(int stepID, SQLResponse sqlResponse) {
        SequenceSQLNode updating = findSQLNode(getSqlIDFromStepID(stepID));
        SequenceNodeOutput currentResult = null;
        if (sqlResponse != null) {
            currentResult = new SequenceNodeOutput(sqlResponse);
            diskResultCache.cacheEntry(getStoreKey(updating), currentResult.getCachedBytes());
        } else {
            currentResult = SequenceNodeOutput.getInstanceFromCachedBytes(diskResultCache.getEntry(getStoreKey(updating)));
        }

        if (updating.child != null) {
            if (updating == first) {
                return updateOptNodeResult((SequenceOptNode) updating.child, currentResult, null);
            } else {
                return updateOptNodeResult((SequenceOptNode) updating.child, null, currentResult);
            }
        } else {
            return currentResult.size();
        }
    }

    private int updateOptNodeResult(SequenceOptNode updating, SequenceNodeOutput optinalLeftParentInput, SequenceNodeOutput optinalRightParentInput) {
        long startTime = System.currentTimeMillis();
        if (optinalLeftParentInput == null) {
            optinalLeftParentInput = loadSequenceNodeOutput(getStoreKey(updating.parents.get(0)));
        }

        if (optinalRightParentInput == null) {
            optinalRightParentInput = loadSequenceNodeOutput(getStoreKey(updating.parents.get(1)));
        }

        ResultOpt opt = updating.getOpt();
        SequenceNodeOutput currentResult;

        if (opt == ResultOpt.INTERSECT) {
            currentResult = SequenceNodeOutput.intersect(optinalLeftParentInput, optinalRightParentInput);
        } else if (opt == ResultOpt.UNION) {
            currentResult = SequenceNodeOutput.union(optinalLeftParentInput, optinalRightParentInput);
        } else if (opt == ResultOpt.BACKWARD_EXCEPT) {
            currentResult = SequenceNodeOutput.except(optinalLeftParentInput, optinalRightParentInput);
        } else if (opt == ResultOpt.FORWARD_EXCEPT) {
            currentResult = SequenceNodeOutput.except(optinalRightParentInput, optinalLeftParentInput);
        } else {
            throw new RuntimeException("Unknown OPT:" + opt);
        }

        String cacheKey = getStoreKey(updating);
        diskResultCache.cacheEntry(cacheKey, currentResult.getCachedBytes());

        logger.info("Time to process and persist:" + (System.currentTimeMillis() - startTime) + " with OPT being " + opt);

        if (updating.child == null) {
            return currentResult.size();
        } else {
            return updateOptNodeResult((SequenceOptNode) updating.child, currentResult, null);
        }

    }

    public SequenceSQLNode findSQLNode(int sqlID) {
        if (sqlID < 0 || sqlID >= sqlNodeCount) {
            throw new IllegalArgumentException("sqlID " + sqlID + " is invalid");
        }

        if (sqlID == 0) {
            return first;
        }
        SequenceNode temp = first;
        for (int i = 0; i < sqlID; i++) {
            temp = temp.child;
        }
        return (SequenceSQLNode) (((SequenceOptNode) temp).parents.get(1));
    }

    public SequenceOptNode findOptNode(int optID) {
        SequenceNode current = first;
        for (int i = 0; i <= optID; i++) {
            current = current.child;
        }
        return (SequenceOptNode) current;
    }

    public SequenceNodeOutput getSequeneFinalResult() {
        SequenceNode resultNode = findLowestChild();
        if (resultNode == null) {
            return null;
        } else {
            return loadSequenceNodeOutput(getStoreKey(resultNode));
        }
    }

    private SequenceNodeOutput loadSequenceNodeOutput(String key) {
        return SequenceNodeOutput.getInstanceFromCachedBytes(diskResultCache.getEntry(key, false));
    }

    private String getStoreKey(SequenceNode node) {
        return sequenceID + "_" + workerID + "_" + node.getIdentifier();
    }

    //must be a SequenceOptNode unless the topology only contains ONE SequenceSQLNode 
    private SequenceNode findLowestChild() {
        SequenceNode lowest = first;
        while (lowest != null && lowest.child != null) {
            lowest = lowest.child;
        }
        return lowest;//may return null
    }

    @Override
    public String toString() {
        String head = "worker " + workerID;
        SequenceNode lowest = findLowestChild();
        if (lowest == null) {
            return head;
        } else {
            String newLine = System.getProperty("line.separator");
            StringBuilder sb = new StringBuilder(head + " sequence expression: " + toString(lowest));
            sb.append(newLine);
            sb.append(newLine);
            for (int i = 0; i < sqlNodeCount; i++) {
                SequenceSQLNode sqlNode = findSQLNode(i);
                SequenceNodeOutput sqlOutput = loadSequenceNodeOutput(getStoreKey(sqlNode));
                sb.append("     sql ").append(sqlNode.getSqlID()).append(": ").append(sqlNode.sql).append(" (result size:").append(sqlOutput.size()).append(")").append(newLine);
            }
            sb.append(newLine);
            for (int i = 0; i < optNodeCount; i++) {
                SequenceOptNode optNode = findOptNode(i);
                SequenceNodeOutput optOutput = loadSequenceNodeOutput(getStoreKey(optNode));
                sb.append("     opt ").append(optNode.getOptID()).append(": ").append(optNode.opt).append(" (result size:").append(optOutput.size()).append(")").append(newLine);
            }
            return sb.toString();
        }
    }

    private String toString(SequenceNode node) {
        if (node instanceof SequenceSQLNode) {
            return " ( " + node.getIdentifier() + " ) ";
        } else {
            SequenceOptNode optNode = (SequenceOptNode) node;
            return " ( " + toString(optNode.parents.get(1)) + " " + optNode.opt + " " + toString(optNode.parents.get(0)) + " ) ";
        }
    }
}
