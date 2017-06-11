import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhzy on 2017/6/8.
  */
object temp_project {


  def main(args: Array[String]): Unit = {

    val conf = new  SparkConf().setAppName("temp-project").setMaster("local");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val wordDataFrame = sqlContext.createDataFrame(
      Seq(
        (0, Array("Hi", "I", "want", "to", "learn", "data", "science")),
        (1, Array("I", "want", "to", "use", "scala")),
        (2, Array("This", "is", "easy", "and", "fan"))
      )
    ).toDF("label", "words")

    val ngrams = new NGram().setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngrams.transform(wordDataFrame)
    ngramDataFrame.take(3).map(line => line.getAs[Stream[String]]("ngrams").toList
        ).foreach((println)
    )
  }
}
