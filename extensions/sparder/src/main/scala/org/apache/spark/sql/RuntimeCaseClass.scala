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

package io.kyligence.kap.query.runtime

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

class RuntimeCaseClass {}

case class CreateTableArgc(gtinfo: String,
                           tableName: String,
                           cuboid: String,
                           schema: StructType,
                           filePath: Seq[String])

case class CreatePushdownTableArgc(gtinfo: String,
                                   tableName: String,
                                   cuboid: String,
                                   schema: StructType,
                                   pushdown: String,
                                   filePath: Seq[String])

case class CreateDictPushdownTableArgc(gtinfo: String,
                                       tableName: String,
                                       cuboid: String,
                                       schema: StructType,
                                       pushdown: String,
                                       binaryFilterPushdown: String,
                                       dict: String,
                                       filePath: Seq[String],
                                       fileFormat: String,
                                       diagnosisWriterType: String)

case class AggArgc(dataFrame: DataFrame, group: List[Column], agg: List[Column])

case class SelectArgc(dataFrame: DataFrame, select: List[Column])

case class OrderArgc(dataFrame: DataFrame, orders: List[Column])
