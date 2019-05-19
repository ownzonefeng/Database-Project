package sampling

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Main {
  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate()

    val rdd = RandomRDDs.uniformRDD(sc, 100000, seed = 8)
    val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, {(f*10).toInt})))

    val table = session.createDataFrame(rdd2, StructType(
      StructField("A1", DoubleType, nullable = false) ::
      StructField("A2", DoubleType, nullable = false) ::
      Nil
    ))

    val desc = new Description
    desc.lineitem = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/lineitem.parquet/*.parquet")
    desc.customer = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/customer.parquet/*.parquet")
    desc.orders = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/order.parquet/*.parquet")
    desc.supplier = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/supplier.parquet/*.parquet")
    desc.nation = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/nation.parquet/*.parquet")
    desc.region = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/region.parquet/*.parquet")
    desc.part = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/part.parquet/*.parquet")
    desc.partsupp = session.read.parquet("/Users/wentao/Downloads/tpch_parquet_sf1/partsupp.parquet/*.parquet")
    desc.e = 0.1
    desc.ci = 0.95

    val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    // check storage usage for samples
    for(i <- desc.samples.indices)
    {
      assert(desc.samples(i).count() <= 1000000)
    }
    // Execute first query
    Executor.execute_Q1(desc, session, List("3"))
  }     
}
