package UDAF

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkSQLUDFUDAF {


  def main (args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("UDAF").setMaster("local[2]")
    //
    //    val sc = new SparkContext(conf)
    //
    //    val sqlContext = new SQLContext(sc)
    val ss = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val peopleRDD = ss.sparkContext.textFile("file/people.txt")

    val schemaString = "name age"

//    readFromJSON(ss)
//    createView(ss)
//    createDataset(ss)
    createDateFrameFromRDD(ss)
  }

  def readFromJSON(ss: SparkSession): Unit ={
    val df = ss.read.json("file/people.json")

    /**
      * 显示整个表
      */
    df.show()

    /**
      * 打印schema
      */
    df.printSchema()

    /**
      * 选择某一列值
      */

    df.select("age").show()

    /**
      * 选择某一列的值并对其进行计算
      */

    import ss.implicits._
    df.select($"name", $"age" + 1).show()

    /**
      * 选择年龄大于21的人
      */
    df.filter($"age" > 21).show()

    /**
      * 根据年龄group并且统计数量
      */
    df.groupBy("age").count().show()
  }

  /**
    * 创建临时图
    * @param ss
    */
  def createView(ss: SparkSession): Unit ={
    val df = ss.read.json("file/people.json")

    /**
      * 创建临时视图   只能存在于一个session中
      */
    df.createOrReplaceTempView("people")

    val sqlDF = ss.sql("select * from people")

    sqlDF.show()

    /**
      * 全局视图  是跨session的存在
      */
    df.createGlobalTempView("people2")

    ss.sql("select * from global_temp.people2").show()

    ss.newSession().sql("select * from global_temp.people2").show()

  }

  /**
    * 创建dataset
    */
  def createDataset(ss: SparkSession): Unit ={

    import ss.implicits._

    /**
      * 直接创建dataSet
      */
    val caseClassDS = Seq(Person("Andy", 32)).toDS()

    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()

    primitiveDS.map(_ + 1).collect()

    val path = "file/people.json"

    val peopleDS = ss.read.json(path).as[Person]

    peopleDS.show()
  }

  case class Person(name:String, age:Long)

  /**
    *
    */
  def createDateFrameFromRDD(ss: SparkSession): Unit ={

    import ss.implicits._
    val peopleDF = ss.sparkContext.textFile("file/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teengersDF = ss.sql("select name, age from people where age between 13 and 19")

    teengersDF.map(teenger => "Name:" + teenger(0)).show()

    teengersDF.map(teenger => "Name" + teenger.getAs[String]("name")).show()

    implicit  val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    teengersDF.map(teenger => teenger.getValuesMap[Any](List("name", "age"))).collect()

  }

  /**
    * 自定义schema
    */
  def userDefinedSchema(ss:SparkSession): Unit ={

    import ss.implicits._

  }


}
