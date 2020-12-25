package saied.spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{StructField, StructType,StringType,LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql._
import org.apache.spark.sql.functions.expr
object Day4 {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Day4")
      .master("local[*]")
      .getOrCreate()
    val dir = "/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json"

    val df = spark.read.format("json").load(dir)
//
//    df.printSchema()
//    df.show(5)

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

//    val df = spark.read.format("json").schema(myManualSchema).load(dir)
//    df.show(5)
//    df.printSchema()

//    println(expr("(((someCol + 5) * 200) - 6) < otherCol"))

    df.first()
  }
}


