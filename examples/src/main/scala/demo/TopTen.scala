package demo

import org.apache.spark.sql.SparkSession

/**
  * //@Author: fansy 
  * //@Time: 2018/8/9 17:32
  * //@Email: fansy1990@foxmail.com
  */
object TopTen {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: TopTen <inputTable> <outputTable> <column>" )
      System.exit(1)
    }
    val input = args(0)
    val output = args(1)
    val column = args(2)
    val spark = SparkSession.builder().enableHiveSupport().appName("Top Ten :"+ input).enableHiveSupport().getOrCreate()
    val data = spark.sql("select * from " + input +" order by "+ column +" desc limit 10")
    val tmpTable = "tmp" + System.currentTimeMillis()
    data.createOrReplaceTempView(tmpTable)
    spark.sql("drop table if exists "+output)
    spark.sql("create table " + output + " as select * from " + tmpTable)

    System.out.println("read from " + input + " to " + output)

    spark.stop()
  }

}
