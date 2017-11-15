package org.apache.spark.sql.udf

import java.nio.ByteBuffer

import com.google.common.collect.Lists
import org.apache.kylin.measure.MeasureAggregator
import org.apache.kylin.metadata.datatype.{DataType, DataTypeSerializer}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.sparder.SparderFunSuite

import scala.util.Random

class SparderCommonBenckmarkV2Test extends SparderFunSuite {

  sparkNeed = false
  test("double agg") {
    val ints = Lists.newArrayList(200000)
    val dataType = DataType.getType("double")
    val serializer = DataTypeSerializer.create(dataType).asInstanceOf[DataTypeSerializer[Any]]

    val measureAggregator = MeasureAggregator.create("min", dataType).asInstanceOf[MeasureAggregator[Any]]

    val buffer = ByteBuffer.allocate(4000)
    val random = new Random(new java.util.Random(10000000))

    val values = Range(0, 2000000).map(value => {
      random.nextDouble() * 10000
    })
      .map { value =>
        buffer.clear()
        serializer.serialize(value, buffer)
        buffer.array().slice(0, buffer.position())
      }
    val row = new GenericInternalRow(1)
    val start = System.currentTimeMillis()
    val value = values.reduce { (oldBytes, newbytes) =>
      buffer.clear()
      measureAggregator.reset()
      val newValue = serializer.deserialize(ByteBuffer.wrap(newbytes))
      measureAggregator.aggregate(newValue)
      val oldValue = serializer.deserialize(ByteBuffer.wrap(oldBytes))
      measureAggregator.aggregate(oldValue)
      val aggregatored = measureAggregator.getState
      serializer.serialize(aggregatored, buffer)
      val bytes = buffer.array().slice(0, buffer.position())
      row.update(0, bytes)
      bytes
    }
    println(serializer.deserialize(ByteBuffer.wrap(value)).asInstanceOf[Double].toLong)
    println(System.currentTimeMillis() - start)
  }


  test("double ") {
    val buffer = ByteBuffer.allocate(4000)

    val dataType = DataType.getType("double")
    val serializer = DataTypeSerializer.create(dataType).asInstanceOf[DataTypeSerializer[Any]]

    serializer.serialize(2054725.1848999998, buffer)
    val value = serializer.deserialize(ByteBuffer.wrap(buffer.array(), 0, buffer.position()))
    println(value)

  }

}
