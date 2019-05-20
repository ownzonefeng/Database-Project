package sampling


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}

import scala.math.{max, min}
import org.apache.spark.sql.functions.column

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Sampler {
  val z_map: mutable.HashMap[Double, Double] = mutable.HashMap(0.8 -> 1.28, 0.9 -> 1.645, 0.95 -> 1.96, 0.98 -> 2.33, 0.99 -> 2.58)  //set hashmap to get z_{alpha/2}
  val queryColumnSet: Seq[Seq[String]] = Seq(Seq("l_shipdate", "l_returnflag", "l_linestatus"), Seq("l_suppkey"), Seq("l_discount", "l_quantity", "l_shipmode"))

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[Row]], List[(Int, Seq[String])]) = {
    val samplesList: ListBuffer[RDD[Row]] = ListBuffer.empty
    val infoList: ListBuffer[(Int, Seq[String])] = ListBuffer.empty

    val z = z_map(ci)
    val error_bound = lineitem.select("l_extendedprice").agg(functions.sum("l_extendedprice")).head().get(0).toString.toDouble * e
    val variance: Double = lineitem.select("l_extendedprice").agg(functions.stddev("l_extendedprice")).head().getDouble(0)
    //variance = 2.2957731090120003E10

    //lower and upper is two bound for binary search for optimal K value

    for(i <- queryColumnSet.indices)   //for every set in qcs get a sample
    {
      val queryColumn: Seq[String] = queryColumnSet(i)
      var lower: Long = 1
      var upper: Long = storageBudgetBytes
      val total_count = lineitem.count()
      var target_ratio_inverse: Int = -1  //the ratio is used to estimate aggregate value in original table
      var target_sampleSet: RDD[Row] = lineitem.rdd
      var final_k: Long = storageBudgetBytes + 100

      // repeat until the lower bound is higher than upper bound which means K is optimal sample size
      while(upper - lower > 1 && lower < storageBudgetBytes)
      {
        //assign a uniformly random value to get sample
        val assignStrata = lineitem.withColumn("X", functions.rand(seed = 8))
        val k: Long = (upper + lower) / 2
        val ratio = k.toDouble / total_count
        val gamma1 = -math.log(0.00005) / total_count // we take 0.00005 as failure rate
        val gamma2 = -(2.0 * math.log(0.00005)) / (3.0 * total_count)
        val q_low = max(0.0, ratio + gamma2 - math.sqrt(gamma2 * gamma2 + 3.0 * gamma2 * ratio))  //q1 in article
        val q_high = min(1.0, ratio + gamma1 + math.sqrt(gamma1 * gamma1 + 2.0 * gamma1 * ratio)) // q2 in article
        val curr_thresh = (q_low, q_high)

        val accept = assignStrata.filter(functions.column("X") <= curr_thresh._1)   // if the random number less than q1 then we accept
        val wait = assignStrata.filter(functions.column("X") <= curr_thresh._2 && functions.column("X") > curr_thresh._1)  // if between q1 and q2, add to waiting list

        val add_number = (accept.count() - k).toInt * (-1)   // the number of samples taking from waiting list
        var sampleSet: DataFrame = lineitem

        //if sample is not enough, union from waiting list, else limit it to k
        if(add_number>0)
        {
          sampleSet = accept.union(wait.orderBy(functions.asc("X")).limit(add_number))
        }
        else
        {
          sampleSet = accept.orderBy(functions.asc("X")).limit(k.toInt)
        }

        //calculate v/n  and  estimated error in moodle
        val stratum_all_count = lineitem.groupBy(queryColumn map column: _*).count().withColumnRenamed("count", "count1")
        val stratum_sample_count = sampleSet.groupBy(queryColumn map column: _*).count().withColumnRenamed("count", "count2")
        val stratum_num = stratum_all_count.count()
        val sample_stratum_num = stratum_sample_count.count()
        val sampleSizeInfo = stratum_sample_count.join(stratum_all_count, queryColumn)
        val sample_count = k.toDouble
        val v = {
          sampleSizeInfo.withColumn("ele", column("count1") * column("count1") / column("count2") * sample_count * variance * variance)
            .agg(functions.sum("ele")).head().getDouble(0)
        }
        val estimated_error: Double = math.sqrt(v / sample_count.toDouble) * z

        // if it satisfied error bound, add the sample to result, and update information
        if(estimated_error < error_bound && stratum_num == sample_stratum_num)
        {
          target_ratio_inverse = (total_count / k).toInt
          target_sampleSet = sampleSet.rdd
          final_k = k
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
