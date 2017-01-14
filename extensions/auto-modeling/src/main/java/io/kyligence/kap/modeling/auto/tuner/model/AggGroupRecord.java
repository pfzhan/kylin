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

package io.kyligence.kap.modeling.auto.tuner.model;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

public class AggGroupRecord implements Comparable<AggGroupRecord> {
    private List<String> dimensions;
    private double score;

    public AggGroupRecord(List<String> dimensions, double score) {
        this.dimensions = dimensions;
        this.score = score;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public double getScore() {
        return score;
    }

    public void increaseScore(double score) {
        this.score += score;
    }

    @Override
    public int compareTo(AggGroupRecord o) {
        double delta = this.getScore() - o.getScore();
        if (delta > 0) {
            return 1;
        } else if (delta < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    public void merge(AggGroupRecord o) {
        this.dimensions = (List<String>) CollectionUtils.union(this.dimensions, o.dimensions);
        this.score = (this.score + o.score) * 0.5;
    }
}
