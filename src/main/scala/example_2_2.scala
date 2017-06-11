import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhzy on 2017/6/4.
  */
object example_2_2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("example__2_2")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/zhzy/desktop/1.txt")
    val pythonLines = lines.filter(line => line.contains("a"))
    print(pythonLines.first())
  }

}
