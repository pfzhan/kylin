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

package io.kyligence.kap.streaming

import io.kyligence.kap.streaming.CreateStreamingFlatTable.castDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class TestDataSourceParser extends AnyFunSuite {

//  +-------+-------------------+--------------+--------+---------+---------+---------+---+----+----+----+----+----+
//  |cust_no|windowDate         |windowDateLong|msg_type|msg_type1|msg_type2|msg_type3|age|bal1|bal2|bal3|bal4|bal5|
//  +-------+-------------------+--------------+--------+---------+---------+---------+---+----+----+----+----+----+
//  |343242 |2021-06-01 00:00:00|1625037465002 |single  |old      |jily     |pandora  |32 |21  |12  |13  |15  |22  |
//  |343222 |2021-06-01 00:00:00|1625037465002 |single  |old      |jily     |pandora  |32 |21  |12  |13  |15  |22  |
//  |343232 |2021-06-01 00:00:00|1625037465002 |single  |old      |jily     |pandora  |32 |21  |12  |13  |15  |22  |
//  |343252 |2021-06-01 00:00:00|1625037465002 |single  |old      |jily     |pandora  |32 |21  |12  |13  |15  |22  |
//  +-------+-------------------+--------------+--------+---------+---------+---------+---+----+----+----+----+----+
  test("test json parser") {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 1)

    val df = spark.read.textFile("./src/streaming/src/test/resources/data/data1.txt")
    val schema = {
      StructType(Seq(StructField("cust_no", StringType, false),
        StructField("windowDate", StringType, false),
        StructField("windowDateLong", StringType, false),
        StructField("msg_type", StringType, false),
        StructField("msg_type1", StringType, false),
        StructField("msg_type2", StringType, false),
        StructField("msg_type3", StringType, false),
        StructField("age", StringType, false),
        StructField("bal1", StringType, false),
        StructField("bal2", StringType, false),
        StructField("bal3", StringType, false),
        StructField("bal4", StringType, false),
        StructField("bal5", StringType, false))
      )
    }
    assert(df.count() == 4)
    val parsedDataframe = castDF(df.toDF(), schema)
    assert(parsedDataframe.count() == 4)
    assert(parsedDataframe.selectExpr("windowDate").head().get(0) == "2021-06-01 00:00:00")
    assert(parsedDataframe.selectExpr("windowDateLong").head().get(0) == "1625037465002")
  }

}
