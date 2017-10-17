package graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


/**
  * Created by zhzy on 2017/8/30.
  */
object graphx_learn {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("mySpark")
    val sc = new SparkContext(conf)

//    var graph: Graph[VertexProperty, String] = null
    val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
    (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
  }

}

class VertexProperty()
case class UserProperty(val name:String) extends VertexProperty
case class ProductProperty(val name:String, val price:Double) extends VertexProperty


