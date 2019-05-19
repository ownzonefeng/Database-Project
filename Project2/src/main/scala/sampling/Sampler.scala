package sampling


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.math.{max, min}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.column

import scala.collection.mutable


object Sampler {
  val z_map: mutable.HashMap[Double, Double] = mutable.HashMap(0.8 -> 1.28, 0.9 -> 1.645, 0.95 -> 1.96, 0.98 -> 2.33, 0.99 -> 2.58)
  val queryColumnSet: Seq[String] = Seq("l_shipdate", "l_returnflag", "l_linestatus")

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    val z = z_map(ci)
    var lower: Long = 1
    var upper: Long = lineitem.count()
    val total_count = upper
    val error_bound = lineitem.select("l_extendedprice").agg(functions.sum("l_extendedprice")).head().get(0).toString.toDouble * e
    var target_ratio: Double = -1.0
    var target_sampleSet: RDD[_] = lineitem.rdd
    val variance = lineitem.select("l_extendedprice").agg(functions.stddev("l_extendedprice")).head().getDouble(0)
    //2.2957731090120003E10
    while(upper - lower > 1 && lower < storageBudgetBytes)
    {
      val assignStrata = lineitem.withColumn("X", functions.rand(seed = 8))
      val k: Long = (lower + upper) / 2
      val ratio = k.toDouble / total_count
      val gamma1 = -math.log(e) / total_count
      val gamma2 = -(2.0 * math.log(e)) / (3.0 * total_count)
      val q_low = max(0.0, ratio + gamma2 - math.sqrt(gamma2 * gamma2 + 3.0 * gamma2 * ratio))
      val q_high = min(1.0, ratio + gamma1 + math.sqrt(gamma1 * gamma1 + 2.0 * gamma1 * ratio))
      val curr_thresh = (q_low, q_high)
      val accept = assignStrata.filter(functions.column("X") <= curr_thresh._1)
      val wait = assignStrata.filter(functions.column("X") <= curr_thresh._2 && functions.column("X") > curr_thresh._1)
      val add_number = (accept.count() - k).toInt * (-1)
      val sampleSet = accept.union(wait.orderBy(functions.asc("X")).limit(add_number))
      val stratum_all_count = lineitem.groupBy(queryColumnSet map column: _*).count().withColumnRenamed("count", "count1").orderBy(functions.asc("count1"))
      val stratum_sample_count = sampleSet.groupBy(queryColumnSet map column: _*).count().withColumnRenamed("count", "count2")
      val stratum_num = stratum_all_count.count()
      val sample_stratum_num = stratum_sample_count.count()
      val sampleSizeInfo = stratum_sample_count.join(stratum_all_count, queryColumnSet).orderBy(functions.asc("count2"))
      val sample_count = k.toDouble
      val v_n = {
        sampleSizeInfo.withColumn("ele", column("count1") * column("count1") / column("count2") * sample_count * variance * variance)
          .agg(functions.sum("ele")).head().getDouble(0)
      }
      val estimated_error: Double = math.sqrt(v_n / sample_count.toDouble) * z
      println(lower, upper, k)
      if(estimated_error < error_bound && stratum_num == sample_stratum_num)
        {
          if(k <= storageBudgetBytes)
            {
              target_ratio = k.toDouble / total_count
              target_sampleSet = sampleSet.rdd
            }
          upper = k
        }
      else
      {
        lower = k
      }
    }
    println(target_ratio)

    (List(target_sampleSet), target_ratio)
  }

}
