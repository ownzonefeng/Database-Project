package sampling


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.math.{max, min}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.column

import scala.collection.mutable

object Sampler {
  val z_map: mutable.HashMap[Double, Double] = mutable.HashMap(0.8 -> 1.28, 0.9 -> 1.645, 0.95 -> 1.96, 0.98 -> 2.33, 0.99 -> 2.58)
  val queryColumnSet: Array[List[String]] = Array(List("l_shipdate", "l_discount", "l_quantity"), List("l_orderkey", "l_returnflag"),
    List("l_shipmode", "l_commitdate", "l_receiptdate"), List("l_partkey", "l_quantity", "l_partkey"))

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    val z = z_map(ci)
    var lower: Long = 1
    var upper: Long = lineitem.count()
    val total_count = upper
    val error_bound = lineitem.select("l_extendedprice").agg(functions.sum("l_extendedprice")).head().getDouble(0) * e
    var target_k: Long = upper
    val assignStrata = lineitem.withColumn("X", functions.rand(seed = 8))
    val variance = lineitem.select("l_extendedprice").agg(functions.stddev("l_extendedprice")).head().getDouble(0)
    while(upper - lower > 1)
    {
      val k: Long = (lower + upper) / 2
      println(lower, upper, k)
      val ratio = k.toDouble / total_count
      val gamma1 = -math.log(e) / total_count
      val gamma2 = -(2.0 * math.log(e)) / (3.0 * total_count)
      val q_low = max(0.0, ratio + gamma2 - math.sqrt(gamma2 * gamma2 + 3.0 * gamma2 * ratio))
      val q_high = min(1.0, ratio + gamma1 + math.sqrt(gamma1 * gamma1 + 2.0 * gamma1 * ratio))
      val curr_thresh = (q_low, q_high)
      val accept = assignStrata.filter(functions.column("X") <= curr_thresh._1)
      val wait = assignStrata.filter(functions.column("X") <= curr_thresh._2 && functions.column("X") > curr_thresh._1)
      val add_number = accept.count() - k
      val sampleSet = accept.union(wait.orderBy(functions.asc("X")).limit(add_number.toInt * (-1)))
      val stratum_all_count = lineitem.groupBy(queryColumnSet(0)(0)).count().withColumnRenamed("count", "count1")
      val stratum_sample_count = sampleSet.groupBy("A2").count().withColumnRenamed("count", "count2")
      val sampleSizeInfo = stratum_sample_count.join(stratum_all_count, "A2")
      val sample_count = stratum_sample_count.select("count2").agg(functions.sum("count2")).head().getLong(0)
      val v_n = {
        sampleSizeInfo.withColumn("ele", column("count1") * column("count1") / column("count2") * sample_count * variance * variance)
          .agg(functions.sum("ele")).head().getDouble(0)
      }
      val estimated_error: Double = math.sqrt(v_n / sample_count.toDouble) * z
      if(estimated_error < error_bound)
        {
          if(k <= storageBudgetBytes)
            {
              target_k = k
            }
          upper = k
        }
      else
      {
        lower = k
      }
    }
    println(target_k)
    null
  }

}
