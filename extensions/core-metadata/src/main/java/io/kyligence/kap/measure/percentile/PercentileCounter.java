/**
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

package io.kyligence.kap.measure.percentile;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

public class PercentileCounter implements Serializable {
    private static final double INVALID_QUANTILE_RATIO = -1;

    double compression;
    double quantileRatio;

    TDigest registers;

    public PercentileCounter(double compression) {
        this(compression, INVALID_QUANTILE_RATIO);
    }

    public PercentileCounter(PercentileCounter another) {
        this(another.compression, another.quantileRatio);
        merge(another);
    }

    public PercentileCounter(double compression, double quantileRatio) {
        this.compression = compression;
        this.quantileRatio = quantileRatio;
        reInitRegisters();
    }

    private void reInitRegisters() {
        this.registers = TDigest.createAvlTreeDigest(this.compression);
    }

    public void add(double v) {
        registers.add(v);
    }

    public void merge(PercentileCounter counter) {
        assert this.compression == counter.compression;
        registers.add(counter.registers);
    }

    public double getResultEstimate() {
        return registers.quantile(quantileRatio);
    }

    public void writeRegisters(ByteBuffer out) {
        registers.compress();
        registers.asSmallBytes(out);
    }

    public void readRegisters(ByteBuffer in) {
        registers = AVLTreeDigest.fromBytes(in);
        compression = registers.compression();
    }

    public int getBytesEstimate() {
        return maxLength();
    }

    public int maxLength() {
        // 10KB for max length
        return 10 * 1024;
    }

    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        AVLTreeDigest.fromBytes(in);
        int total = in.position() - mark;
        in.position(mark);
        return total;
    }

    public void clear() {
        reInitRegisters();
    }
}
