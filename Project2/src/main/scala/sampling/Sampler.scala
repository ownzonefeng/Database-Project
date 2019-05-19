package sampling


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}

import scala.math.{max, min}
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Sampler {
  val z_map: mutable.HashMap[Double, Double] = mutable.HashMap(0.8 -> 1.28, 0.9 -> 1.645, 0.95 -> 1.96, 0.98 -> 2.33, 0.99 -> 2.58)
//  val queryColumnSet: Seq[Seq[String]] = Seq(Seq("l_shipdate", "l_returnflag", "l_linestatus"), Seq("l_shipdate"))
  val queryColumnSet: Seq[Seq[String]] = Seq(Seq("l_shipdate", "l_returnflag", "l_linestatus"), Seq("l_shipdate"))

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[Row]], _) = {
    val samplesList: ListBuffer[RDD[Row]] = ListBuffer.empty
    val infoList: ListBuffer[(Int, Seq[String])] = ListBuffer.empty

    val z = z_map(ci)
    val error_bound = lineitem.select("l_extendedprice").agg(functions.sum("l_extendedprice")).head().get(0).toString.toDouble * e
    val variance: Double = lineitem.select("l_extendedprice").agg(functions.stddev("l_extendedprice")).head().getDouble(0)
    //variance = 2.2957731090120003E10

    var lower: Long = 1
    var upper: Long = lineitem.count()
    val total_count = upper

    for(i <- queryColumnSet.indices)
    {
      val queryColumn: Seq[String] = queryColumnSet(i)

      var target_ratio_inverse: Int = -1
      var target_sampleSet: RDD[Row] = lineitem.rdd
      var final_k: Long = storageBudgetBytes + 100

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
        var sampleSet: DataFrame = lineitem
        if(add_number>0)
        {
          sampleSet = accept.union(wait.orderBy(functions.asc("X")).limit(add_number))
        }
        else
        {
          sampleSet = accept.orderBy(functions.asc("X")).limit(k.toInt)
        }

        val stratum_all_count = lineitem.groupBy(queryColumn map column: _*).count().withColumnRenamed("count", "count1").orderBy(functions.asc("count1"))
        val stratum_sample_count = sampleSet.groupBy(queryColumn map column: _*).count().withColumnRenamed("count", "count2")
        val stratum_num = stratum_all_count.count()
        val sample_stratum_num = stratum_sample_count.count()
        val sampleSizeInfo = stratum_sample_count.join(stratum_all_count, queryColumn).orderBy(functions.asc("count2"))
        val sample_count = k.toDouble
        val v = {
          sampleSizeInfo.withColumn("ele", column("count1") * column("count1") / column("count2") * sample_count * variance * variance)
            .agg(functions.sum("ele")).head().getDouble(0)
        }
        val estimated_error: Double = math.sqrt(v / sample_count.toDouble) * z
        println(k)
        if(estimated_error < error_bound && stratum_num == sample_stratum_num)
        {
          if(k <= storageBudgetBytes)
          {
            target_ratio_inverse = (total_count / k).toInt
            target_sampleSet = sampleSet.rdd
            final_k = k
          }
          upper = k
        }
        else
        {
          lower = k
        }
      }
      if(final_k <= storageBudgetBytes)
        {
          samplesList.+=(target_sampleSet)
          infoList.+=((target_ratio_inverse, queryColumn))
        }
    }
    (samplesList.toList, infoList.toList)
  }

}
