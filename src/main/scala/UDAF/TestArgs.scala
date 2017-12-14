package UDAF

import scala.io.Source

object TestArgs {

  def main(args: Array[String]): Unit = {

    val config = loadJson(args(0))
    println(config)

  }

  def loadJson(path: String) = Source.fromFile(path)("UTF-8").getLines().mkString("")
}
