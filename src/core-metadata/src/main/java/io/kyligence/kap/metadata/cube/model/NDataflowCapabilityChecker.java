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

package io.kyligence.kap.metadata.cube.model;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.cuboid.NLookupCandidate;
import io.kyligence.kap.metadata.cube.cuboid.NQueryLayoutChooser;

public class NDataflowCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowCapabilityChecker.class);

    public static CapabilityResult check(NDataflow dataflow, SQLDigest digest) {
        logger.info("Matching Layout in dataflow {}, SQL digest {}", dataflow, digest);
        CapabilityResult result = new CapabilityResult();
        if (digest.limitPrecedesAggr) {
            logger.info("Exclude NDataflow {} because there's limit preceding aggregation", dataflow);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        // 1. match joins is ensured at model select
        String rootFactTable = dataflow.getModel().getRootFactTableName();
        IRealizationCandidate chosenCandidate = null;
        if (digest.joinDescs.isEmpty() && !rootFactTable.equals(digest.factTable)) {
            chosenCandidate = tryMatchLookup(dataflow, digest, result);
            if (chosenCandidate != null) {
                logger.info("Matched table {} snapshot in dataflow {} ", digest.factTable, dataflow);
            }
        } else {
            // for query-on-facttable
            Pair<NLayoutCandidate, List<CapabilityResult.CapabilityInfluence>> candidateAndInfluence = NQueryLayoutChooser
                    .selectCuboidLayout(//
                            dataflow.getLatestReadySegment(), digest);
            if (candidateAndInfluence != null) {
                chosenCandidate = candidateAndInfluence.getFirst();
                result.influences.addAll(candidateAndInfluence.getSecond());
                if (chosenCandidate != null) {
                    logger.info("Matched layout {} snapshot in dataflow {} ", chosenCandidate, dataflow);
                }
            }
        }
        if (chosenCandidate != null) {
            result.capable = true;
            result.setSelectedCandidate(chosenCandidate);
        } else {
            result.capable = false;
        }
        return result;
    }

    private static IRealizationCandidate tryMatchLookup(NDataflow dataflow, SQLDigest digest, CapabilityResult result) {
        // query from snapShot table
        if (dataflow.getLatestReadySegment() == null)
            return null;

        if (!dataflow.getLatestReadySegment().getSnapshots().containsKey(digest.factTable)) {
            logger.info("Exclude NDataflow {} because snapshot of table {} does not exist", dataflow, digest.factTable);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.NOT_EXIST_SNAPSHOT);
            result.capable = false;
            return null;
        }

        //1. all aggregations on lookup table can be done
        Set<TblColRef> colsOfSnapShot = Sets
                .newHashSet(dataflow.getModel().findFirstTable(digest.factTable).getColumns());
        Collection<TblColRef> unmatchedCols = Sets.newHashSet(digest.allColumns);
        if (!unmatchedCols.isEmpty()) {
            unmatchedCols.removeAll(colsOfSnapShot);
        }

        if (!unmatchedCols.isEmpty()) {
            logger.info("Exclude NDataflow {} because unmatched dimensions [{}] in Snapshot", dataflow, unmatchedCols);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedDimensions(unmatchedCols);
            return null;
        } else {
            return new NLookupCandidate(digest.factTable, true);
        }
    }
}