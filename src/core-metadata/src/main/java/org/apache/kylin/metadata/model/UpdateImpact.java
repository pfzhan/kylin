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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

/**
 *  Updating cc expression will cause change on both cc and measure.
 *  This UpdateImpact records changed cc's and measure's id during cc modification in
 *  io.kyligence.kap.rest.service.ModelSemanticHelper#updateModelColumns.
 */

@Getter
@Setter
public class UpdateImpact implements Serializable {
    private Set<Integer> removedOrUpdatedCCs = new HashSet<>();

    private Set<Integer> invalidMeasures = new HashSet<>();            // removed measure due to cc update, clear related layouts

    private Set<Integer> invalidRequestMeasures = new HashSet<>();     // removed measure in request not in modelDesc

    private Set<Integer> updatedCCs = new HashSet<>();

    private Set<Integer> updatedMeasures = new HashSet<>();

    private Map<Integer, Integer> replacedMeasures = new HashMap<>();  // need to swap measure id in Agg/TableIndex, layouts

    public UpdateImpact() {  // do nothing
    }

    public Set<Integer> getAffectedIds() {
        Set<Integer> affectedId = new HashSet<>();
        affectedId.addAll(updatedCCs);
        affectedId.addAll(updatedMeasures);
        affectedId.addAll(replacedMeasures.values());
        return affectedId;
    }

}
