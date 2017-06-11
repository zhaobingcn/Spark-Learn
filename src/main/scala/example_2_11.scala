import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhzy on 2017/6/4.
  */
object example_2_11 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("example__2_11")
    val sc = new SparkContext(conf)

    val input = sc.textFile("src/main/resources/README");

    val words = input.flatMap(line => line.split(" "))

    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x+y}

    print(counts)
  }

}
