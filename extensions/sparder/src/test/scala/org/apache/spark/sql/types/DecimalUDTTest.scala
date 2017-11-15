package org.apache.spark.sql.types

import java.math.BigDecimal

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSqlFunSuite}



class DecimalUDTTest extends SparkSqlFunSuite {

  test("DecimalUDTTest") {
    val sparkContext = spark.sparkContext
    UDTRegistration.register(classOf[java.math.BigDecimal].getName, classOf[DecimalUDT].getName)
    spark.conf.set("spark.sql.codegen.wholeStage", value = false)
    val rowRDD1 = sparkContext.parallelize(Seq(Row(1, new BigDecimal(1))))
    rowRDD1.take(1).foreach(println)
    val schema1 = StructType(Array(StructField("label", IntegerType, false),
      StructField("point", DecimalUDT(10, 2))))
    val rowRDD2 = sparkContext.parallelize(Seq(Row(2, new BigDecimal(9))))
    val schema2 = StructType(Array(StructField("label", IntegerType, false),
      StructField("point", DecimalUDT(10, 2))))
    val df1 = spark.createDataFrame(rowRDD1, schema1)
    df1.show(10)
    val df2 = spark.createDataFrame(rowRDD2, schema2)
    val rows = df1.union(df2).orderBy("label").take(10)
    rows.foreach(println)
    checkAnswer(
      df1.union(df2).orderBy("label"),
      Seq(Row(1, new BigDecimal(1)), Row(2, new BigDecimal(9)))
    )
  }


  test("sd") {
    val conf = new SparkConf()
    val spark2 = spark
    import spark2.implicits._

    // Encoders are created for case classes
    //spark2.createDataset(Seq(Person("Andy", 32,true))).show()
  }
}
