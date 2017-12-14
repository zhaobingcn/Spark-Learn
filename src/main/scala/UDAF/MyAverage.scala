package UDAF

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MyAverage extends UserDefinedAggregateFunction{


  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

object MyAverage{

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    ss.udf.register("myAverage", new MyAverage)

    val df = ss.read.json("file/employee.json")
    df.createOrReplaceTempView("employees")
    df.show()

    val schema = df.schema
    val newschema = schema.add(StructField("average", DoubleType, true))

    val result = ss.sql("select *,myAverage(salary) over (partition by name order by time rows between 4 preceding and current row) from employees")

    val result2 = ss.sql("select *,myAverage(salary) over (partition by name order by time rows between 2 preceding and current row) as a from employees")

    //    val result3 = ss.sql("")
    result2.show()
    //    println(newschema)
    //    df.registerTempTable("t1")
    //    ss.sql("create temporary function myAverage as 'MyAverage'")
    //    val result = ss.sql("select myAverage(salary) as average_salary from t1")
    //    result.show()
  }
}


