package simjoin

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
      val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
      val ctx = new SparkContext(sparkConf)
      val rdd = ctx.parallelize(List(("hello","world"),("good","morning"), ("hello","morning"), ("hello","world"), ("hello","morning"))).distinct()
      rdd.collect().foreach(println)
    }

  }
