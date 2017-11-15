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

package org.apache.spark.sql.execution.datasources.sparder

import java.io.ObjectInputStream
import java.util

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexSpliceReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._
import scala.collection.mutable


class SparderFileFormatSuite extends SparderFunSuite with BeforeAndAfterEach {
  val gtinfoHex = "2e6f72672e6170616368652e6b796c696e2e637562652e677269647461626c652e43756265436f646553797374656d0001008e013caced0005737200256f72672e6170616368652e6b796c696e2e64696d656e73696f6e2e4461746544696d456e6300000000000000010c00007872002d6f72672e6170616368652e6b796c696e2e64696d656e73696f6e2e41627374726163744461746544696d456e6300000000000000010c00007872002c6f72672e6170616368652e6b796c696e2e64696d656e73696f6e2e44696d656e73696f6e456e636f64696e6700000000000000010c00007870770400000003737200376f72672e6170616368652e6b796c696e2e64696d656e73696f6e2e4461746544696d456e63244461746544696d56616c7565436f64656300000000000000010200014c0008646174617479706574002d4c6f72672f6170616368652f6b796c696e2f6d657461646174612f64617461747970652f44617461547970653b787070780e4375626f69642032303937313532110464617465ffff06626967696e74ffff06626967696e74ffff07646563696d616c130407646563696d616c130407646563696d616c130404686c6c630aff04686c6c630aff04746f706e6404066269746d6170ffff0e657874656e646564636f6c756d6e64ff0e657874656e646564636f6c756d6e64ff0e657874656e646564636f6c756d6e64ff03726177ffff03726177ffff03726177ffff07646563696d616c1304000101040101033e000102c0030200fc000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

  override def beforeEach() {
    val conf = new SparkConf()
    spark = SparkSession.builder
      .master("local[1]")
      .appName("test-sql-context")
      .config(conf)
      .getOrCreate()
    format = new SparderFileFormat()
    hadoopConf = spark.sparkContext.hadoopConfiguration
  }

  override def afterEach() {

  }


  test("testInferSchema") {
    val option = mutable.HashMap.empty[String, String]
    option.put(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gtInfoStr)
    option.put(SparderConstants.TABLE_ALIAS, alias)

    val status = path.getFileSystem(hadoopConf).getFileStatus(path)
    val structType = format.inferSchema(spark, option.toMap, List(status))
    val structType2 = StructType(Seq(
      StructField("col_0", BinaryType, false),
      StructField("col_1", BinaryType, false),
      StructField("col_2", BinaryType, false),
      StructField("col_3", BinaryType, false),
      StructField("col_4", BinaryType, false),
      StructField("col_5", BinaryType, false),
      StructField("col_6", BinaryType, false),
      StructField("col_7", BinaryType, false),
      StructField("col_8", BinaryType, false),
      StructField("col_9", BinaryType, false),
      StructField("col_10", BinaryType, false),
      StructField("col_11", BinaryType, false),
      StructField("col_12", BinaryType, false),
      StructField("col_13", BinaryType, false),
      StructField("col_14", BinaryType, false),
      StructField("col_15", BinaryType, false),
      StructField("col_16", BinaryType, false),
      StructField("col_17", BinaryType, false),
      StructField("col_18", BinaryType, false),
      StructField("col_19", BinaryType, false)
    ))

    assert(structType.get.toString() == structType2.toString())
  }


  test("test select error col  ") {
    spark.read.format("org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
      .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gtInfoStr)
      .option(SparderConstants.CUBOID_ID, cuboid)
      .option(SparderConstants.TABLE_ALIAS, alias)
      .load(path.toUri.toString)
      .createOrReplaceTempView("test")
    intercept[AnalysisException](spark.sql("select col_error from test").show(10))
  }

  import org.apache.spark.sql.functions._

  test("test parquet") {
    //    val df = spark.range(1, 1000000).coalesce(1)
    //    df.selectExpr("id AS a","1 as b", "3 as c").write.parquet("/tmp/spark-test/data/spark-18539")
    val schema = StructType(Seq(StructField("a", LongType), StructField("b", LongType), StructField("c", LongType)))
    spark.read.option("mergeSchema", "true").schema(schema).parquet("/tmp/spark-test/data/spark-18539").filter("a > 1000").count()

  }
  test("test parquet2") {
    //    val df = spark.range(1, 10000).coalesce(1)
    //    df.selectExpr("id AS a").write.parquet("/tmp/spark-test/data/spark-18539")
    val schema = StructType(Seq(StructField("a", LongType)))

    val df = spark.read.option("mergeSchema", "true").schema(schema)
      .parquet("/tmp/spark-test/data/spark-18539")
      .filter(expr("a > 1000").and(expr("a < 1200")))
    df.show(100)
    println(df.queryExecution.optimizedPlan)
  }


  test("test parquet 2") {
    val spark2 = spark
    import spark2.implicits._

    //    case class Person(name:String, age:Int)
    //    //val s = Seq(Person("Roger", 100), Person("Ttt", 10)).toDS()
    //    val s = Seq( Person("Ttt", 10)).toDS()

    // Encoders are created for case classes
    val df = Seq(Person("10000823", true, "2010-04-03"), Person("10000002", false, "2011-03-31")).toDS()

    //df.select(expr("cast(name as double)").gt(lit(new java.math.BigDecimal("10000002")))).show(100)
    df.select(add_months(col("datee"), 1)).show(100)
    //df.flatMap(f => Seq(f.name, f.age, f.age + 1)).show(100)
    //    df.flatMap(_.name.split(" ")).show(100)
    //    df.explode()
    //    df.select(col("age"), explode(split(col("name"), " ")).as("word")).show(100)
    //    df.flatMap(f=>f.name)
    //    df.schema

    //    val result = caseClassDS.groupBy(expr("length(name)")).agg(expr("count( distinct age)"))
    //
    //    result.show()


    //val s = Seq(1,2,3).toDS()
    //s.show()
  }
  test("test parquet 2-read") {
    val spark2 = spark
    import org.apache.spark.sql.functions._
    spark.read.parquet("/tmp/spark-test/data/spark-2").filter(expr("name = 'Andy'").and(expr("age = 32")))
      .show(100)
    //    case class Person(name:String, age:Int)
    //    //val s = Seq(Person("Roger", 100), Person("Ttt", 10)).toDS()
    //    val s = Seq( Person("Ttt", 10)).toDS()


    // Encoders are created for case classes
    //    val caseClassDS = Seq(Person("Andy", 32), Person("Auto", 33), Person(null, 34)).toDS().write.parquet("/tmp/spark-test/data/spark-2")

    //    val result = caseClassDS.groupBy(expr("length(name)")).agg(expr("count( distinct age)"))
    //
    //    result.show()


    //val s = Seq(1,2,3).toDS()
    //s.show()
  }
  test("test parquet 3-read") {
    val spark2 = spark
    import org.apache.spark.sql.functions._
    spark.read.parquet("/tmp/spark-test/data/spark-2").filter(expr("name = 'Andy'"))
      .show(100)
    //    case class Person(name:String, age:Int)
    //    //val s = Seq(Person("Roger", 100), Person("Ttt", 10)).toDS()
    //    val s = Seq( Person("Ttt", 10)).toDS()


    // Encoders are created for case classes
    //    val caseClassDS = Seq(Person("Andy", 32), Person("Auto", 33), Person(null, 34)).toDS().write.parquet("/tmp/spark-test/data/spark-2")

    //    val result = caseClassDS.groupBy(expr("length(name)")).agg(expr("count( distinct age)"))
    //
    //    result.show()


    //val s = Seq(1,2,3).toDS()
    //s.show()
  }

  test("select limit 1 ") {
    spark.read.format("org.apache.spark.sql.execution.datasources.sparder.SparderFileFormat")
      .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, gtInfoStr)
      .option(SparderConstants.CUBOID_ID, cuboid)
      .option(SparderConstants.TABLE_ALIAS, alias)
      .load(path.toUri.toString)
      .createOrReplaceTempView("test")
    val rows = spark.sql("select col_0, col_1, col_2, col_3, col_4, col_5 from test limit 1 ")
    QueryTest.checkAnswer(rows, Seq(
      Row(Array[Byte](65, 66, 73, 78, 9, 9, 9, 9, 9, 9, 9, 9),
        Array[Byte](0),
        Array[Byte](0),
        Array[Byte](7, -36),
        Array[Byte](69),
        Array[Byte](-114, -104, 78)))
    )
    print(rows)
  }


  test("read cubemapper info ") {
    var map: util.Map[Long, util.Set[String]] = null
    val inputStream = cubeInfoPath.getFileSystem(new Configuration()).open(cubeInfoPath)
    val obj: ObjectInputStream = new ObjectInputStream(inputStream)
    try {
      map = obj.readObject.asInstanceOf[util.Map[Long, util.Set[String]]]
    } finally if (inputStream != null) inputStream.close()
    assert(map.get(114696L).asScala ==
      Set(
        "hdfs://sandbox.hortonworks.com:8020/kylin/kylin_default_instance2/parquet/8372c3b7-a33e-4b69-83dd-0bb8b1f8117e/fb5980b6-d678-4ecd-b191-bcd499f254c9/peoLwWAlpT.parquettar",
        "hdfs://sandbox.hortonworks.com:8020/kylin/kylin_default_instance2/parquet/8372c3b7-a33e-4b69-83dd-0bb8b1f8117e/fb5980b6-d678-4ecd-b191-bcd499f254c9/xDlOPIMkJr.parquettar"
      ))
  }

  test("find cuboid file ") {
    assert(getCubeFileName(cuboid).map(sandboxPath => sandboxPath.getName).toList
      == List("peoLwWAlpT.parquettar", "xDlOPIMkJr.parquettar"))
  }

  def getCubeFileName(cubeid: Long): Seq[Path] = {
    var map: util.Map[Long, util.Set[String]] = null
    val inputStream = cubeInfoPath.getFileSystem(new Configuration()).open(cubeInfoPath)
    try {
      val obj: ObjectInputStream = new ObjectInputStream(inputStream)
      try {
        map = obj.readObject.asInstanceOf[util.Map[Long, util.Set[String]]]
      } finally if (inputStream != null) inputStream.close()
    }
    val paths = map.get(cubeid)
    paths.asScala.map(new Path(_)).toSeq
  }


  test("read file cuboid mapping") {
    val inputStream = path.getFileSystem(new Configuration()).open(path)
    val indexLength = inputStream.readLong

    val pageIndexSpliceReader = new ParquetPageIndexSpliceReader(inputStream, indexLength, ParquetFormatConstants.KYLIN_PARQUET_TARBALL_HEADER_SIZE)

    val readers = pageIndexSpliceReader.getIndexReaderByCuboid(cuboid)

  }


  test(" map join  test") {

    import org.apache.spark.sql.functions._
    val sc = spark.sparkContext
    val employeesRDD = sc.parallelize(Seq(
      Employee("Mary", 33, "IT"),
      Employee("Paul", 45, "IT"),
      Employee("Peter", 26, "MKT"),
      Employee("Jon", 34, "MKT"),
      Employee("Sarah", 29, "IT"),
      Employee("Steve", 21, "Intern")
    ))

    var seq = mutable.ListBuffer.empty[Department]


    seq.append(Department(s"IT", "IT  Department"))
    seq.append(Department(s"MKT", "Marketing Department"))
    seq.append(Department(s"FIN", "Finance & Controlling"))
    for (i <- Range(0, 1300)) {
      seq.append(Department(s"${i}IT", "IT  Department"))
      seq.append(Department(s"${i}MKT", "Marketing Department"))
      seq.append(Department(s"${i}FIN", "Finance & Controlling"))
    }
    val departmentsDF = spark.createDataFrame(seq)
    val tmpDepartments = broadcast(departmentsDF.as("departments").repartition(1).cache())
    val employeesDF = spark.createDataFrame(employeesRDD)


    val broadcastSeq = sc.broadcast(seq)
    val tmpDepartments2 = departmentsDF.as("departments").cache()
    println(tmpDepartments.queryExecution)

    spark.createDataFrame(new util.ArrayList[Row], tmpDepartments.schema)

    tmpDepartments.show(100)
    val df = employeesDF.join(tmpDepartments,
      col("depId") === col("id"), // join by employees.depID == departments.id
      "inner").groupBy("name").sum("age")
    println(df.queryExecution)
    for (i <- Range(0, 100)) {
      val startTime = System.currentTimeMillis()
      df.count()
      println(System.currentTimeMillis() - startTime)
    }
  }


  test("test df ") {
    import org.apache.spark.sql.functions._
    val sc = spark.sparkContext
    val employeesRDD = sc.parallelize(Seq(
      Money(171.6f),
      Money(28.17f)
    ))

    val employeesDF = spark.createDataFrame(employeesRDD)
    employeesDF.select("mon").agg(expr("sum(mon)")).show()
  }
}

case class Money(mon: Double)

case class Person(name: String, isMan: Boolean, datee: String)

case class Employee(name: String, age: Int, depId: String)

case class Department(id: String, dpName: String)
