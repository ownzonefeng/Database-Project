package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    // Task 1
    val Bottom = rdd.map(
      tuple => (index.map(ind => tuple(ind)).mkString(",") , (tuple.getInt(indexAgg).toDouble, 1.toDouble))  //for every row, get the (key, agg_value, 1)
    ).reduceByKey(do_agg(agg), reducers)  //reduce according to agg, return (key, agg_value*, counts)

    val upper = Bottom.flatMap(
      tuple => (0 to groupingAttributes.length).toList.flatMap(  //make a number for combining
        num => tuple._1.split(",").combinations(num).toList.map(  //split the key and make combination
          li=> (li.mkString(","), tuple._2)   //for every list in combination result, create (key*, agg_value*, counts)
        )
      )
    ).reduceByKey(do_agg(agg), reducers)  //aggregate same key and calculate, return (key*, agg_value*, counts*)

    //make (key*, agg_value*, counts*) -> (key*, value*)
    val result: RDD[(String, Double)] = agg match{
      case "AVG" => upper.map(tuple => (tuple._1, tuple._2._1/tuple._2._2))
      case "COUNT" => upper.map(tuple => (tuple._1, tuple._2._2))
      case  _    => upper.map(tuple => (tuple._1, tuple._2._1))
    }

    result
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    // naive algorithm for cube computation
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    val upper = rdd.flatMap(tuple =>
      (0 to groupingAttributes.length).toList.flatMap(    //iterate all combination
        num => index.map(ind => tuple(ind)).combinations(num).toList.map(
          li => (li.mkString(","), (tuple.getInt(indexAgg).toDouble, 1.toDouble)) //make them as ((key, agg_value, 1))
        )
      )
    ).reduceByKey(do_agg(agg), reducers)

    //make (key*, agg_value*, counts*) -> (key*, value*)
    val result: RDD[(String, Double)] = agg match{
      case "AVG" => upper.map(tuple => (tuple._1, tuple._2._1/tuple._2._2))
      case "COUNT" => upper.map(tuple => (tuple._1, tuple._2._2))
      case  _    => upper.map(tuple => (tuple._1, tuple._2._1))
    }

    result
  }

  def do_agg(agg: String): ((Double, Double),(Double, Double)) => (Double, Double) = agg match{
    case "MIN" => (left: (Double, Double), right: (Double, Double)) => if (left._1 < right._1) left else right
    case "MAX" => (left: (Double, Double), right: (Double, Double)) => if (left._1 > right._1) left else right
    case  _    => (left: (Double, Double), right: (Double, Double)) => (left._1 + right._1, left._2 + right._2)
  }

}
