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
package org.apache.spark.benchmark

import io.kyligence.kap.engine.spark.job.FirstUDAF
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.udaf.{EncodeApproxCountDistinct, EncodePreciseCountDistinct, ReuseApproxCountDistinct, ReusePreciseCountDistinct}
import org.apache.spark.util.Benchmark

import scala.concurrent.duration._
import org.apache.spark.sql.functions._

class UDAFBenchmark extends SparderBaseFunSuite with SharedSparkSession {


  ignore("bitmap new uadf vs old udaf ") {
    val N = 2 << 15
    val benchmark = new Benchmark(
      name = "bitmap new uadf vs old udaf",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 5.seconds,
      minTime = 10.seconds,
      outputPerIteration = true
    )


    spark.udf.register("bitmap", new FirstUDAF("COUNT_DISTINCT", DataType.getType("bitmap"), true))
    spark.range(N).createOrReplaceTempView("t")
    benchmark.addCase("old udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.table("t").agg(callUDF("bitmap", col("id"))).collect()
    }

    benchmark.addCase("new udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.table("t").agg(new Column(EncodePreciseCountDistinct(col("id").expr).toAggregateExpression())).collect()
    }

    benchmark.addCase("old udaf w/ group by w/4") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      spark.table("t").groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(callUDF("bitmap", col("id")))
        .collect()
    }

    benchmark.addCase("new af w/ group by w/4 ") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      spark.table("t").groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(new Column(EncodePreciseCountDistinct(col("id").expr).toAggregateExpression()))
        .collect()
    }

    benchmark.addCase("old udaf w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      spark.table("t").groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(callUDF("bitmap", col("id")))
        .collect()
    }

    benchmark.addCase("new af w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      spark.table("t").groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(new Column(EncodePreciseCountDistinct(col("id").expr).toAggregateExpression()))
        .collect()
    }

    benchmark.run()

    //    Java HotSpot(TM) 64-Bit Server VM 1.8.0_181-b13 on Mac OS X 10.14.2
    //    Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz
    //
    //    bitmap new uadf vs old udaf:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    //      ------------------------------------------------------------------------------------------------
    //      old udaf w/o group by                           68 /  148          1.0        1032.9       1.0X
    //      new udaf w/o group by                           28 /   60          2.3         433.3       2.4X
    //      old udaf w/ group by                            85 /  150          0.8        1298.7       0.8X
    //      new af w/ group by w/4 fallback                 54 /  104          1.2         825.1       1.3X
    //      old udaf w/ group by w/100                      61 /  115          1.1         937.3       1.1X
    //      new af w/ group by w/100 fallback               45 /   81          1.4         692.4       1.5X

  }

  ignore("reuse new bitmap udf vs old bitmap udaf") {
    val N = 2 << 15
    val benchmark = new Benchmark(
      name = "reuse new bitmap udf vs old bitmap udaf",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 5.seconds,
      minTime = 10.seconds,
      outputPerIteration = true
    )

    val df = spark.range(N).groupBy("id")
      .agg(new Column(EncodePreciseCountDistinct(col("id").expr).toAggregateExpression()).as("id2"))
      .cache()
    spark.udf.register("bitmap", new FirstUDAF("COUNT_DISTINCT", DataType.getType("bitmap"), false))
    spark.range(N).createOrReplaceTempView("t")
    benchmark.addCase("old udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.agg(callUDF("bitmap", col("id2"))).collect()
    }

    benchmark.addCase("new udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.agg(new Column(ReusePreciseCountDistinct(col("id2").expr).toAggregateExpression())).collect()
    }

    benchmark.addCase("old udaf w/ group by w/4") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(callUDF("bitmap", col("id2")))
        .collect()
    }

    benchmark.addCase("new af w/ group by w/4 ") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      df.groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(new Column(ReusePreciseCountDistinct(col("id2").expr).toAggregateExpression()))
        .collect()
    }

    benchmark.addCase("old udaf w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(callUDF("bitmap", col("id2")))
        .collect()
    }

    benchmark.addCase("new af w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      df.groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(new Column(ReusePreciseCountDistinct(col("id2").expr).toAggregateExpression()))
        .collect()
    }

    benchmark.run()

    //    Java HotSpot(TM) 64-Bit Server VM 1.8.0_181-b13 on Mac OS X 10.14.2
    //    Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz
    //
    //    reuse new bitmap udf vs old bitmap udaf: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    //      ------------------------------------------------------------------------------------------------
    //      old udaf w/o group by                           59 /  150          1.1         894.0       1.0X
    //      new udaf w/o group by                           35 /   82          1.9         537.1       1.7X
    //      old udaf w/ group by                            84 /  162          0.8        1277.3       0.7X
    //      new af w/ group by w/4 fallback                 74 /  133          0.9        1135.5       0.8X
    //      old udaf w/ group by w/100                      99 /  139          0.7        1509.3       0.6X
    //      new af w/ group by w/100 fallback               48 /   88          1.4         726.4       1.2X
  }

  ignore("new hllc udf vs old hllc udaf") {
    val N = 2 << 15
    val benchmark = new Benchmark(
      name = "new hllc udf vs old hllc udaf",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 5.seconds,
      minTime = 10.seconds,
      outputPerIteration = true
    )


    spark.udf.register("hllc", new FirstUDAF("COUNT_DISTINCT", DataType.getType("hllc(16)"), true))
    spark.range(N).createOrReplaceTempView("t")
    benchmark.addCase("old udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.table("t").agg(callUDF("hllc", col("id"))).collect()
    }

    benchmark.addCase("new udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.table("t").agg(new Column(EncodeApproxCountDistinct(col("id").expr, 16).toAggregateExpression())).collect()
    }

    benchmark.addCase("old udaf w/ group by w/4") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      spark.table("t").groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(callUDF("hllc", col("id")))
        .collect()
    }

    benchmark.addCase("new af w/ group by w/4 ") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      spark.table("t").groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(new Column(EncodeApproxCountDistinct(col("id").expr, 16).toAggregateExpression()))
        .collect()
    }

    benchmark.addCase("old udaf w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      spark.table("t").groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(callUDF("hllc", col("id")))
        .collect()
    }
    benchmark.addCase("new af w/ group by w/100 ") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      spark.table("t").groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(new Column(EncodeApproxCountDistinct(col("id").expr, 16).toAggregateExpression()))
        .collect()
    }

    benchmark.run()

    //    new hllc udf vs old hllc udaf:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    //      ------------------------------------------------------------------------------------------------
    //      old udaf w/o group by                         8836 / 8994          0.0      134822.1       1.0X
    //      new udaf w/o group by                           38 /   84          1.7         573.2     235.2X
    //      old udaf w/ group by w/4                        94 /  149          0.7        1435.9      93.9X
    //      new af w/ group by w/4 fallback                 93 /  137          0.7        1418.6      95.0X
    //      old udaf w/ group by w/100                     198 /  283          0.3        3023.3      44.6X
    //      new af w/ group by w/100 fallback               60 /   99          1.1         914.1     147.5X
  }

  ignore("reuse new hllc udf vs old hlc udaf") {
    val N = 2 << 15
    val benchmark = new Benchmark(
      name = "reuse new hllc udf vs old hlc udaf",
      valuesPerIteration = N,
      minNumIters = 5,
      warmupTime = 5.seconds,
      minTime = 10.seconds,
      outputPerIteration = true
    )

    val df = spark.range(N).groupBy("id")
      .agg(new Column(EncodeApproxCountDistinct(col("id").expr, 16).toAggregateExpression()).as("id2"))
      .cache()
    df.collect()
    spark.udf.register("hllc", new FirstUDAF("COUNT_DISTINCT", DataType.getType("hllc(16)"), false))
    benchmark.addCase("old udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.agg(callUDF("hllc", col("id2"))).collect()
    }

    benchmark.addCase("new udaf w/o group by") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      df.agg(new Column(ReuseApproxCountDistinct(col("id2").expr, 16).toAggregateExpression())).collect()
    }

    benchmark.addCase("old udaf w/ group by w/4") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(callUDF("hllc", col("id2")))
        .collect()
    }

    benchmark.addCase("new af w/ group by w/4 ") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      df.groupBy(col("id").divide(4).cast("BIGINT"))
        .agg(new Column(ReuseApproxCountDistinct(col("id2").expr, 16).toAggregateExpression()))
        .collect()
    }

    benchmark.addCase("old udaf w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "false")
      df.groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(callUDF("hllc", col("id2")))
        .collect()
    }
    benchmark.addCase("new af w/ group by w/100") { _ =>
      spark.conf.set(SQLConf.USE_OBJECT_HASH_AGG.key, "true")
      spark.conf.set(SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key, "2")
      df.groupBy(col("id").divide(100).cast("BIGINT"))
        .agg(new Column(ReuseApproxCountDistinct(col("id2").expr, 16).toAggregateExpression()))
        .collect()
    }

    benchmark.run()

    //    reuse new hllc udf vs old hlc udaf:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    //      ------------------------------------------------------------------------------------------------
    //      old udaf w/o group by                        9946 / 10532          0.0      151770.7       1.0X
    //      new udaf w/o group by                           30 /   76          2.2         460.0     329.9X
    //      old udaf w/ group by w/4                        58 /  130          1.1         880.5     172.4X
    //      new af w/ group by w/4 fallback                 50 /  100          1.3         768.7     197.4X
    //      old udaf w/ group by w/100                     184 /  257          0.4        2810.2      54.0X
    //      new af w/ group by w/100 fallback               43 /   75          1.5         650.1     233.5X
  }


}
