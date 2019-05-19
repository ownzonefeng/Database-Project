package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._
import java.time.LocalDateTime

import distance._
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}


object Main {
  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val inputFile="../dblp_small.csv"
    val numAnchors = 4
    val distanceThreshold = 2
    val attrIndex = 0    
        
    val input = new File(getClass.getResource(inputFile).getFile).getPath    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)   
    
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(input)      
    
    val rdd = df.rdd        
    val schema = df.schema.toList.map(x => x.name)    
    val dataset = new Dataset(rdd, schema)

    val startTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now)
    println(startTime)

    
    //Similarity join
    println("similarity join")
    val t1 = System.nanoTime
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)

    val resultSize = res.count
    println(resultSize)
    val t2 = System.nanoTime

    println((t2-t1)/ Math.pow(10,9))

    // cartesian
    println("cartesian")
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => x(attrIndex)).cartesian(rdd.map(x => x(attrIndex)))
      .filter(x => x._1.toString != x._2.toString && editDistance(x._1.toString, x._2.toString) <= distanceThreshold)

    println(cartesian.count())
    val t2Cartesian = System.nanoTime
    println((t2Cartesian-t1Cartesian)/ Math.pow(10,9))

  }     
}
