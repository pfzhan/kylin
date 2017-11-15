package org.apache.spark.sql.execution.datasources.sparder

import org.scalatest.FunSuite

class BasicTestSuite extends FunSuite {

  test("foo test") {

    val nestedNumbers = List((1, 'a'), (1, 'b'), (3, 'd')).toMap


    val map = Map(1 -> List("a", "b"), 2 -> List("c", "d"))
    val flatMap = map.toList.flatMap(
      x => {
        val map1 = x._2.map(s => (x._1, s))
        println(map1)
        map1
      })


    println()

    //    val fruits: List[String] = List("apples", "oranges", "pears", "bananas")
    //    //> fruits  : List[String] = List(apples, oranges, pears, bananas)
    //
    //    val oddFruitsIterator =
    //      Iterator.from(1, 2).takeWhile(_ < fruits.size).map(fruits(_))
    //    //> oddFruits  : Iterator[String] = non-empty iterator
    //
    //    oddFruitsIterator.foreach(println)
    //    //> oranges
    //    //> bananas
  }
}
