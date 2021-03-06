package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import distance._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger: Logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = _
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {

    rdd = dataset.getRDD().map(x => x(attrIndex).toString)
    val count: Long = rdd.count()
    val probability: Double = numAnchors.toDouble / count
    val anchors = rdd.sample(withReplacement = false, fraction = probability, seed = 8)
    // anchors.collect().foreach(println)
    val anchorsIndex = anchors.zipWithIndex() // get the anchors
    val cartesianProduct = rdd.cartesian(anchorsIndex).map(x => (x._1, (x._2._2, x._2._1, editDistance(x._1, x._2._1)))) // compute the distance between each point and each anchor
    //cartesianProduct.collect().foreach(println)
    val clusterAssign = cartesianProduct.reduceByKey((x, y) => if(x._3 < y._3) x else y).map(x => (x._1, x._2._1, x._2._2, x._2._3)) // find the cluster center for each point
    val outerPartition = clusterAssign.cartesian(anchorsIndex).map(x => (x._1._1, x._1._2, x._2._1, x._2._2, x._1._4)).map(x =>
    {
      // find the outer partition with the method in the following thesis
      // Y. Wang, A. Metwally, and S. Parthasarathy. Scalable all-pairs similarity search in metric spaces. In Proceedings of KDD, 2013.
      if(x._2 == x._4)
      {
        (x._4, (x._1, "inner"))
      }
      else
      {
        if( (((x._2 + x._4) % 2 == 1) ^ (x._2 < x._4)) && (editDistance(x._1,x._3) <= x._5 + 2 * distThreshold))
        {
          (x._4, (x._1, "outer"))
        }
        else
          {
            (-1, ("-1", "invalid"))
          }
      }
    }).filter(x => x._1 != -1)
    val clusterMember = outerPartition.partitionBy(new HashPartitioner(numAnchors))
    val similarPairs = clusterMember.mapPartitions(x =>
    {
      // find the similar pairs in each cluster
      var result: ListBuffer[(String, String)] = ListBuffer.empty
      var inner: ListBuffer[String] = ListBuffer.empty

      while(x.hasNext)
        {
          val pair = x.next()
          val currentPoint = pair._2._1
          val typeOfPoint = pair._2._2
          for(joinPoint <- inner)
            {
              if(editDistance(joinPoint, currentPoint) <= distThreshold && editDistance(joinPoint, currentPoint) > 0)
                {
                  result.+=((joinPoint, currentPoint))
                  result.+=((currentPoint, joinPoint))
                }
            }

          inner.+=(currentPoint)

        }
      result.iterator
    }).repartition(1)

    similarPairs.distinct()
  }

}