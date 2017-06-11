import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
/**
  * Created by zhzy on 2017/1/24.
  */
object SparkSqlDemo {

  def main(args: Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("SparkSqlDemo")

    sparkConf.setMaster("local")

    var spark = SparkSession.builder().appName("SparkSqlDemo").config(sparkConf).getOrCreate()

//    runJDBCDataSource(spark);
//    loadDataSourceFromJSon(spark);
//    loadDataFromParquet(spark)
    runFromRdd(spark)
  }


  private def runJDBCDataSource(spark: SparkSession): Unit = {

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?user=root&password=654321")
      .option("dbtable", "users")//表名
      .load()
    //从数据库中查询出name,age,uid三列存储，以parqet的形式存储
//    jdbcDF.select("name","age","uid").write.format("parquet").save("./out/resultedParquet")
    //从数据库中查询出name,age,uid三列存储，以json的形式存储
//    jdbcDF.select("name", "age", "uid").write.format("json").save("./out/resultedNewJson")
    jdbcDF.select("uid", "name", "age").write.mode("overwrite").saveAsTable("user_copy")
    val jdbcSQL = spark.sql("select * from user_copy where name like 'zhao%'")
    jdbcSQL.show()
    jdbcSQL.write.format("json").save("./out/SparkSqlDemo")
  }

  private def loadDataSourceFromJSon(spark: SparkSession): Unit = {
      //从runJDBCDataSource中产生的json数据在这里读取
    val jsonDF = spark.read.json("./out/SparkSqlDemo/user.json");
    //输出结构
    jsonDF.printSchema();
    //创建临时视图
    jsonDF.createOrReplaceTempView("user")
    //从临时视图进行查询
    val namesDF = spark.sql("select * from user where name like 'zhao%'");
    import spark.implicits._;
    //操作查询结果，在每个查询结果之前加name,但是使用该方法的时候必须导入implicits
    namesDF.map(attribute => "Name:" + attribute(1)).show()
    //将结果以json的形式写入到./out/resaultedJson中
//    jsonDF.select("name").write.format("json").save("./out/resultedJson")
    namesDF.write.format("json").save("./out/resultedJson/Name.json");

  }

  //列式存储
  private def loadDataFromParquet(spark: SparkSession): Unit={
    //读取从runJDBCDataSource中产生的数据
    val parquetDF = spark.read.load("./out/resultedParquet/data.parquet")
    //输出结构
    parquetDF.printSchema()
    //创建临时视图
    parquetDF.createOrReplaceTempView("user")
    val namesDF = spark.sql("select * from user where uid > 0")
    namesDF.show()
    namesDF.write.format("parquet").save("./out/resultedParquet/newParquet")

  }


  private def runFromRdd(spark: SparkSession): Unit = {
    //创建一个JSON格式的RDD
    val jsonRDD = spark.sparkContext.makeRDD("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    //从rdd中读取数据
    val readRDD = spark.read.json(jsonRDD)
    readRDD.show()
  }

}
