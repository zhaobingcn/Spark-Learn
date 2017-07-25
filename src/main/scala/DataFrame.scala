import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by zhzy on 2017/7/24.
  */
object DataFrame {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder().master("local[*]").config("spark.driver.bindAddress", "127.0.0.1")
      .appName("sparkSql")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read.json("file/people.json")
    //展现全部数据
//    df.show()
    //展示数据格式
//    df.printSchema()
    //选择出一列
//    df.select("name").show()
    //聚合操作
//    df.groupBy("age").count().show()

    //普通temporary
//    df.createOrReplaceTempView("people")
//    val sqlDF = spark.sql("select * from people")
//    sqlDF.show()

    //global temporary
//    df.createGlobalTempView("people")
//    spark.sql("select * from global_temp.people").show()
//    spark.newSession().sql("SELECT * FROM global_temp.people").show()


    //创建dataset
//    val caseClassDS = Seq(Person("Andy", 32)).toDS()
//    caseClassDS.show()

//    val primitiveDS = Seq(1,2,3).toDS()
//    primitiveDS.map(_ + 1).collect()

    //DataFrame转化为DataSet
//    val path = "file/people.json"
//    val peopleDS = spark.read.json(path).as[Person]
//    peopleDS.show()


    //使用case class创建新的dataframe
//    val peopleDF = spark.sparkContext
//      .textFile("file/people.txt")
//      .map(_.split(","))
//      .map(attributes => Person(attributes(0),attributes(1).trim.toInt))
//      .toDF();
//
//    peopleDF.createOrReplaceTempView("people")
//    val  teenagersDF = spark.sql("select name, age from people where age between 13 and 19")
//    teenagersDF.map(teenager => "Name:" + teenager(0)).show()
//    teenagersDF.map(teenager => "Name" + teenager.getAs[String]("name"))

    //使用schema创建dataframe
    val peopleRDD = spark.sparkContext.textFile("file/people.txt")

    val shemaString = "name age"

    val fields = shemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val rowRDD = peopleRDD.map(_.split(","))
      .map(at => Row(at(0), at(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results = spark.sql("select name from people")

    results.map((at => "Name:" + at(0))).show()


  }

  case class Person(name:String, age:Long)


}
