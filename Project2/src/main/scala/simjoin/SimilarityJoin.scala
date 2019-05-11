package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import distance._

import scala.collection.mutable.ArrayBuffer

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
    val anchors = rdd.sample(false, fraction = probability, seed = 8)
    val cartesianProduct = rdd.subtract(anchors).cartesian(anchors)

    val clusterAssign = {cartesianProduct.map(x => (x._1, (x._2, editDistance(x._1, x._2))))
      .reduceByKey((x, y) => if(x._2 < y._2) x else y).map(x => (x._1, x._2._2))}

    val outerPartition =  {clusterAssign.cartesian(anchors).map(x => (x._1._1, x._1._2, x._2, editDistance(x._1._1, x._2)))
      .filter(x => x._4 <= x._2 + 2 * distThreshold).map(x => (x._3, x._1))}

    val cluster = {outerPartition.groupByKey().map(x => (x._1, x._2.toBuffer.+=(x._1))).map(_._2)
      .flatMap(x => x.combinations(2)).map(x => (x.head,x(1)))}

    val similarPairs = {cluster.map(x => (x._1, x._2, editDistance(x._1, x._2))).filter(x => x._3 <= distThreshold)
      .flatMap(x => List((x._1, x._2), (x._2, x._1))).distinct()}

    similarPairs

  }

}

