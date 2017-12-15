package UDF

import UDAF.MyAverage
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types._

class FMax(query : String) extends UserDefinedAggregateFunction with Discrete {
  override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("max", DoubleType) :: StructField("count", LongType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Double.MinValue
    buffer(1) = 0l
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(input.getDouble(0) > buffer.getDouble(0))
      buffer(0) = input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer2.getDouble(0) > buffer1.getDouble(0))
      buffer1(0) = buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): String = {
//    val discrete_func = discrete_NINF_INF(1)
//    discrete_func(buffer.getDouble(0)).toString
    if(buffer.getLong(1) > 0){
      buffer.getDouble(0).toString
    }
    else
      "NaN"
  }
}

object FMax{
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    ss.udf.register("fmax", new FMax)

    val df = ss.read.json("file/employee.json")
    df.createOrReplaceTempView("employees")
    df.show()

    val result = ss.sql("select *,fmax(salary) over (partition by name order by time rows between 2 preceding and 1 preceding) from employees")

    result.show()

  }
}
