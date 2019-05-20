/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package io.kyligence.kap.engine.spark.job

import org.apache.hadoop.fs.ContentSummary
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.mockito.Mockito.{when, mock => jmock}

class TestSparkCubingStep extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  test("auto set driver memory by cuboid num") {
    assert(NSparkCubingStep.computeDriverMemory(10) == "1024m")
    assert(NSparkCubingStep.computeDriverMemory(100) == "1024m")
    assert(NSparkCubingStep.computeDriverMemory(1000) == "1152m")
    assert(NSparkCubingStep.computeDriverMemory(5000) == "1664m")
    assert(NSparkCubingStep.computeDriverMemory(10000) == "2304m")
    assert(NSparkCubingStep.computeDriverMemory(50000) == "4096m")
    assert(NSparkCubingStep.computeDriverMemory(100000) == "4096m")
  }
}
