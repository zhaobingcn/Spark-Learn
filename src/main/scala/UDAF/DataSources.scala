package UDAF

import org.apache.spark.sql.SparkSession

object DataSources {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    /**
      * 读取parquet文件并且选择其中的几列存储，仍然存储为parquet
      */
//    val useDF = ss.read.load("/Users/zhzy/IdeaProjects/prophet-ce/spark-domain/src/test/resources/tmp/single_sql/data")
    val useDF = ss.read.load("/Users/zhzy/IdeaProjects/prophet-ce/spark-domain/src/test/resources/data/sql/dl1")
//    val usersDF = ss.read.load("/Users/zhzy/ProfessionalSoftware/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/users.parquet")
//    usersDF.select("name", "favorite_color").write.save("file/nameAndFavColors.parquet")

    /**
      * 读取json存为parquet
      */
//    val peopleDF = ss.read.format("json").load("file/people.json")
//    peopleDF.select("name", "age").write.format("parquet").save("file/namesAndAges.parquet")

    /**
      * 直接读取parquet文件可以直接使用Sql
      */
//    import ss.implicits._
//    val sqlDF = ss.sql("SELECT * FROM parquet.'file/nameAndFavColors.parquet'")
//
//    sqlDF.show()

    /**
      * 把peopleDF持久化到磁盘上
      */
//    val peopleDF = ss.read.format("json").load("file/people.json")
//    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

    /**
      * 把UserDF持久化到磁盘上
      */
//    val usersDF = ss.read.load("/Users/zhzy/ProfessionalSoftware/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/users.parquet")
//    usersDF.write.partitionBy("favorite_color").format("parquet").save("file/namesPartByColor.parquet")

  }

}
