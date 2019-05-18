package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._


object test{

  def main(args: Array[String])
  {
    val queryColumnSet = Array(Array("l_shipdate", "l_discount", "l_quantity"), Array("l_orderkey", "l_returnflag"),
      Array("l_shipmode", "l_commitdate", "l_receiptdate"), Array("l_partkey", "l_quantity", "l_partkey"))
    println(queryColumnSet(0).toList.flatten)
  }

}
