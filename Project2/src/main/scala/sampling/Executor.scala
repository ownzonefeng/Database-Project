package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import java.sql.Date
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}



object Executor {
  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 1)
    val queryLang: String = s"select\n\tl_returnflag,\n\tl_linestatus,\n\tsum(l_quantity) as sum_qty,\n\tsum(l_extendedprice) as sum_base_price,\n\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n\tavg(l_quantity) as avg_qty,\n\tavg(l_extendedprice) as avg_price,\n\tavg(l_discount) as avg_disc,\n\tcount(*) as count_order\nfrom\n\tlineitem\nwhere\n\tl_shipdate <= date '1998-12-01' - interval ${params.head} day \ngroup by\n\tl_returnflag,\n\tl_linestatus\norder by\n\tl_returnflag,\n\tl_linestatus"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    session.sql(queryLang).show()
  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    val queryLang: String = s"select\n\tl_orderkey,\n\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n\to_orderdate,\n\to_shippriority\nfrom\n\tcustomer,\n\torders,\n\tlineitem\nwhere\n\tc_mktsegment = ${params(0)}\n\tand c_custkey = o_custkey\n\tand l_orderkey = o_orderkey\n\tand o_orderdate < date ${params(1)}\n\tand l_shipdate > date ${params(1)}\ngroup by\n\tl_orderkey,\n\to_orderdate,\n\to_shippriority\norder by\n\trevenue desc,\n\to_orderdate"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    session.sql(queryLang).show()
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tn_name,\n\tsum(l_extendedprice * (1 - l_discount)) as revenue\nfrom\n\tcustomer,\n\torders,\n\tlineitem,\n\tsupplier,\n\tnation,\n\tregion\nwhere\n\tc_custkey = o_custkey\n\tand l_orderkey = o_orderkey\n\tand l_suppkey = s_suppkey\n\tand c_nationkey = s_nationkey\n\tand s_nationkey = n_nationkey\n\tand n_regionkey = r_regionkey\n\tand r_name = ${params(0)}\n\tand o_orderdate >= date ${params(1)}\n\tand o_orderdate < date ${params(1)} + interval '1' year\ngroup by\n\tn_name\norder by\n\trevenue desc"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    session.sql(queryLang).show()
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tsum(l_extendedprice * l_discount) as revenue\nfrom\n\tlineitem\nwhere\n\tl_shipdate >= date '${params(0)}'\n\tand l_shipdate < date '${params(0)}' + interval '1' year\n\tand l_discount between ${params(1)} - 0.01 and ${params(1)} + 0.01\n\tand l_quantity < ${params(2)}"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    session.sql(queryLang).show()
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tsupp_nation,\n\tcust_nation,\n\tl_year,\n\tsum(volume) as revenue\nfrom\n\t(\n\t\tselect\n\t\t\tn1.n_name as supp_nation,\n\t\t\tn2.n_name as cust_nation,\n\t\t\textract(year from l_shipdate) as l_year,\n\t\t\tl_extendedprice * (1 - l_discount) as volume\n\t\tfrom\n\t\t\tsupplier,\n\t\t\tlineitem,\n\t\t\torders,\n\t\t\tcustomer,\n\t\t\tnation n1,\n\t\t\tnation n2\n\t\twhere\n\t\t\ts_suppkey = l_suppkey\n\t\t\tand o_orderkey = l_orderkey\n\t\t\tand c_custkey = o_custkey\n\t\t\tand s_nationkey = n1.n_nationkey\n\t\t\tand c_nationkey = n2.n_nationkey\n\t\t\tand (\n\t\t\t\t(n1.n_name = '${params(0)}' and n2.n_name = '${params(1)}')\n\t\t\t\tor (n1.n_name = '${params(1)}' and n2.n_name = '${params(0)}')\n\t\t\t)\n\t\t\tand l_shipdate between date '1995-01-01' and date '1996-12-31'\n\t) as shipping\ngroup by\n\tsupp_nation,\n\tcust_nation,\n\tl_year\norder by\n\tsupp_nation,\n\tcust_nation,\n\tl_year"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    session.sql(queryLang).show()
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tnation,\n\to_year,\n\tsum(amount) as sum_profit\nfrom\n\t(\n\t\tselect\n\t\t\tn_name as nation,\n\t\t\textract(year from o_orderdate) as o_year,\n\t\t\tl_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n\t\tfrom\n\t\t\tpart,\n\t\t\tsupplier,\n\t\t\tlineitem,\n\t\t\tpartsupp,\n\t\t\torders,\n\t\t\tnation\n\t\twhere\n\t\t\ts_suppkey = l_suppkey\n\t\t\tand ps_suppkey = l_suppkey\n\t\t\tand ps_partkey = l_partkey\n\t\t\tand p_partkey = l_partkey\n\t\t\tand o_orderkey = l_orderkey\n\t\t\tand s_nationkey = n_nationkey\n\t\t\tand p_name like '%${params(0)}%'\n\t) as profit\ngroup by\n\tnation,\n\to_year\norder by\n\tnation,\n\to_year desc"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tc_custkey,\n\tc_name,\n\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n\tc_acctbal,\n\tn_name,\n\tc_address,\n\tc_phone,\n\tc_comment\nfrom\n\tcustomer,\n\torders,\n\tlineitem,\n\tnation\nwhere\n\tc_custkey = o_custkey\n\tand l_orderkey = o_orderkey\n\tand o_orderdate >= date '${params(0)}'\n\tand o_orderdate < date '${params(0)}' + interval '3' month\n\tand l_returnflag = 'R'\n\tand c_nationkey = n_nationkey\ngroup by\n\tc_custkey,\n\tc_name,\n\tc_acctbal,\n\tc_phone,\n\tn_name,\n\tc_address,\n\tc_comment\norder by\n\trevenue desc"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tps_partkey,\n\tsum(ps_supplycost * ps_availqty) as value\nfrom\n\tpartsupp,\n\tsupplier,\n\tnation\nwhere\n\tps_suppkey = s_suppkey\n\tand s_nationkey = n_nationkey\n\tand n_name = '${params(0)}'\ngroup by\n\tps_partkey having\n\t\tsum(ps_supplycost * ps_availqty) > (\n\t\t\tselect\n\t\t\t\tsum(ps_supplycost * ps_availqty) * ${params(1)}\n\t\t\tfrom\n\t\t\t\tpartsupp,\n\t\t\t\tsupplier,\n\t\t\t\tnation\n\t\t\twhere\n\t\t\t\tps_suppkey = s_suppkey\n\t\t\t\tand s_nationkey = n_nationkey\n\t\t\t\tand n_name = '${params(0)}'\n\t\t)\norder by\n\tvalue desc"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tl_shipmode,\n\tsum(case\n\t\twhen o_orderpriority = '1-URGENT'\n\t\t\tor o_orderpriority = '2-HIGH'\n\t\t\tthen 1\n\t\telse 0\n\tend) as high_line_count,\n\tsum(case\n\t\twhen o_orderpriority <> '1-URGENT'\n\t\t\tand o_orderpriority <> '2-HIGH'\n\t\t\tthen 1\n\t\telse 0\n\tend) as low_line_count\nfrom\n\torders,\n\tlineitem\nwhere\n\to_orderkey = l_orderkey\n\tand l_shipmode in ('${params(0)}', '${params(1)}')\n\tand l_commitdate < l_receiptdate\n\tand l_shipdate < l_commitdate\n\tand l_receiptdate >= date '${params(2)}'\n\tand l_receiptdate < date '${params(2)}' + interval '1' year\ngroup by\n\tl_shipmode\norder by\n\tl_shipmode;"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tsum(l_extendedprice) / 7.0 as avg_yearly\nfrom\n\tlineitem,\n\tpart\nwhere\n\tp_partkey = l_partkey\n\tand p_brand = '${params(0)}'\n\tand p_container = '${params(1)}'\n\tand l_quantity < (\n\t\tselect\n\t\t\t0.2 * avg(l_quantity)\n\t\tfrom\n\t\t\tlineitem\n\t\twhere\n\t\t\tl_partkey = p_partkey\n\t)"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tc_name,\n\tc_custkey,\n\to_orderkey,\n\to_orderdate,\n\to_totalprice,\n\tsum(l_quantity)\nfrom\n\tcustomer,\n\torders,\n\tlineitem\nwhere\n\to_orderkey in (\n\t\tselect\n\t\t\tl_orderkey\n\t\tfrom\n\t\t\tlineitem\n\t\tgroup by\n\t\t\tl_orderkey having\n\t\t\t\tsum(l_quantity) > ${params(0)}\n\t)\n\tand c_custkey = o_custkey\n\tand o_orderkey = l_orderkey\ngroup by\n\tc_name,\n\tc_custkey,\n\to_orderkey,\n\to_orderdate,\n\to_totalprice\norder by\n\to_totalprice desc,\n\to_orderdate"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\tsum(l_extendedprice* (1 - l_discount)) as revenue\nfrom\n\tlineitem,\n\tpart\nwhere\n\t(\n\t\tp_partkey = l_partkey\n\t\tand p_brand = '${params(0)}'\n\t\tand p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n\t\tand l_quantity >= ${params(3)} and l_quantity <= ${params(3)} + 10\n\t\tand p_size between 1 and 5\n\t\tand l_shipmode in ('AIR', 'AIR REG')\n\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n\t)\n\tor\n\t(\n\t\tp_partkey = l_partkey\n\t\tand p_brand = '${params(1)}'\n\t\tand p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n\t\tand l_quantity >= ${params(4)} and l_quantity <= ${params(4)} + 10\n\t\tand p_size between 1 and 10\n\t\tand l_shipmode in ('AIR', 'AIR REG')\n\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n\t)\n\tor\n\t(\n\t\tp_partkey = l_partkey\n\t\tand p_brand = '${params(2)}'\n\t\tand p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n\t\tand l_quantity >= ${params(5)} and l_quantity <= ${params(5)} + 10\n\t\tand p_size between 1 and 15\n\t\tand l_shipmode in ('AIR', 'AIR REG')\n\t\tand l_shipinstruct = 'DELIVER IN PERSON'\n\t)"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    val queryLang: String = s"select\n\ts_name,\n\ts_address\nfrom\n\tsupplier,\n\tnation\nwhere\n\ts_suppkey in (\n\t\tselect\n\t\t\tps_suppkey\n\t\tfrom\n\t\t\tpartsupp\n\t\twhere\n\t\t\tps_partkey in (\n\t\t\t\tselect\n\t\t\t\t\tp_partkey\n\t\t\t\tfrom\n\t\t\t\t\tpart\n\t\t\t\twhere\n\t\t\t\t\tp_name like '${params(0)}%'\n\t\t\t)\n\t\t\tand ps_availqty > (\n\t\t\t\tselect\n\t\t\t\t\t0.5 * sum(l_quantity)\n\t\t\t\tfrom\n\t\t\t\t\tlineitem\n\t\t\t\twhere\n\t\t\t\t\tl_partkey = ps_partkey\n\t\t\t\t\tand l_suppkey = ps_suppkey\n\t\t\t\t\tand l_shipdate >= date '${params(1)}'\n\t\t\t\t\tand l_shipdate < date '${params(1)}' + interval '1' year\n\t\t\t)\n\t)\n\tand s_nationkey = n_nationkey\n\tand n_name = '${params(2)}'\norder by\n\ts_name"
    val test: StructType = desc.lineitem.schema
    val lineitem: DataFrame = session.createDataFrame(desc.samples.head, test)
    lineitem.createOrReplaceTempView("lineitem")
    desc.part.createOrReplaceTempView("part")
    desc.customer.createOrReplaceTempView("customer")
    desc.orders.createOrReplaceTempView("orders")
    desc.supplier.createOrReplaceTempView("supplier")
    desc.nation.createOrReplaceTempView("nation")
    desc.region.createOrReplaceTempView("region")
    desc.part.createOrReplaceTempView("part")
    desc.partsupp.createOrReplaceTempView("partsupp")
    session.sql(queryLang).show()
  }
}
