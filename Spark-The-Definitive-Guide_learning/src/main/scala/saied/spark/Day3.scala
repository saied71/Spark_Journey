package saied.spark
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._
object Day3 {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("Day3")
      .master("local[*]")
      .getOrCreate()

    val df = spark.range(500).toDF("number")
    df.show()
    df.printSchema()

    df.select(df.col("number")+10).show(5)
    println(spark.range(5).toDF().collect())

    
  }
}
