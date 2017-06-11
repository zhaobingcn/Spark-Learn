import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhzy on 2017/1/23.
  */
object MySpark {

  def main(args: Array[String]){

    val conf = new SparkConf().setMaster("local").setAppName("mySpark")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4,5,6)).map(_*3);
    val mappedRDD = rdd.filter(_>10).collect()
    println(rdd.reduce(_+_))
    for(arg <- mappedRDD)
      print(arg+ " ")
    println()
    println("math is work")
  }
}
