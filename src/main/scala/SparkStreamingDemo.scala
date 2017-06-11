

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by zhzy on 2017/1/23.
  */
object SparkStreamingDemo {

  def main(args : Array[String]): Unit ={

    //创建Spark实例
    val sparkConf = new SparkConf().setAppName("Streaming")
    sparkConf.setMaster("local")

    //创建sparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建rdd queue
    val rddQueue = new mutable.Queue[RDD[String]]()
    //从rdd队列中读取输入流
    val inputStream = ssc.queueStream(rddQueue)
    //将输入流的每个元素，每个元素都是一个String,后面加一个a，并返回一个新的rdd

    val mappedStream = inputStream.map(x => (x + 'a', 1))

    //reduceByKey(_+_)对每个元素统计次数。map(x => (x._2, x._1))是将map的key和value交换位置，后面是
    //过滤出现次数超过一次并且String等于"testa"的字符串
    val reduceStream = mappedStream.reduceByKey(_+_).map(x => (x._2, x._1)).filter((x) => x._1>1).
      filter((x) => x._2.equals("zhaobinga"))

    reduceStream.print();
    //将每次的计算结果存储在./out/resulted中
    reduceStream.saveAsTextFiles("./out/resulted")

    ssc.start()
    val seq = conn()
    println(seq)
    //将seq生成的RDD然后放入Spark的streaming的rdd队列，作为输入流

    for(i <- 1 to 3){
      rddQueue.synchronized(
        rddQueue += ssc.sparkContext.makeRDD(seq, 10)
      )
      Thread.sleep(3000)
    }
    ssc.stop()
  }

  //送数据库中取出每个用户的名字,是一个有序队列
  def conn():Seq[String] = {
    val user = "root"
    val password = "654321"
    val host = "localhost"
    val database = "test"
    val conn_str = "jdbc:mysql://" +host+ ":3306/" +database+ "?user=" +user+ "&password=" + password
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = DriverManager.getConnection(conn_str)
    var setName = Seq("")
    try{
      //configure to be read only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      //execute query,查询用户表，有name属性
      val rs = statement.executeQuery("select * from users")
      //interate over resultset

      while(rs.next()){
        val name = rs.getString("name");
        setName = setName :+ name
      }
      return setName;
    }finally {
      conn.close()
    }

  }
}
