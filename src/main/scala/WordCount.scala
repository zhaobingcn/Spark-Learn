import com.google.common.io.Files
import org.apache.spark.SparkContext

/**
  * Created by zhzy on 2017/6/8.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val inpath = "src/main/resources/README"
    val outpath = "out/wordcount_output"
     val sc = new SparkContext("local", "WordCount")
    try{
      val input = sc.textFile(inpath);
      val wc = input
        .map(_.toLowerCase)
        .flatMap(text => text.split("""\W+"""))
        .groupBy(word => word)
        .mapValues(group => group.size)
      println("writing to outpath")
      wc.saveAsTextFile(outpath)
      Console.in.read()
    }finally {
      sc.stop()
    }
  }
}
