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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.common.SparderBaseFunSuite

// scalastyle:off
class SparderEnvTest extends SparderBaseFunSuite {

  test("getExecutorNum when dynamicAllocation enabled") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.shuffle.service.port", "7337")

    assert(5 == SparderEnv.getExecutorNum(sparkConf))
  }

  test("getExecutorNum when dynamicAllocation disabled") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "5")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "0")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.shuffle.service.port", "7337")

    assert(1 == SparderEnv.getExecutorNum(sparkConf))
  }

}

