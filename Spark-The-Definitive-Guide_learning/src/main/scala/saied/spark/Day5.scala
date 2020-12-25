package saied.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StringType, StructType, LongType}
import org.apache.spark.sql.functions.{expr, col,column}
import org.apache.spark.sql.functions.lit
import org.apache.log4j._
object Day5 {
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Day5")
      .master("local[*]")
      .getOrCreate()

    val data_dir = "/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json"

    val df = spark.read.format("json").load(data_dir)
    df.show()
    df.printSchema()

    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType,true),
      new StructField("names", LongType,false)
    ))

    val myRows = Seq(Row("hello", null, 1L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDF = spark.createDataFrame(myRDD,myManualSchema)
    myDF.show()

    import spark.implicits._
    val myDF1 = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")
    myDF1.show()

    df.select("DEST_COUNTRY_NAME").show(2)

    df.select(
      df.col("DEST_COUNTRY_NAME"),
      $"DEST_COUNTRY_NAME",
      column("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      'DEST_COUNTRY_NAME,
      expr("DEST_COUNTRY_NAME")).show(2)

    df.select(expr("DEST_COUNTRY_NAME AS fuck")).show(2)

    df.select(expr("DEST_COUNTRY_NAME AS FUCK").alias("DEST_COUNTRY_NAME"))
      .show(2)

    df.selectExpr("DEST_COUNTRY_NAME as STH", "DEST_COUNTRY_NAME").show(2)

    df.selectExpr(
      "*",
      ("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as Within_Country")
    ).show(2)

    df.selectExpr("avg(count)", "count(ORIGIN_COUNTRY_NAME)").show(2)
    df.selectExpr("avg(count)", "count(distinct(ORIGIN_COUNTRY_NAME))")
      .withColumn("Average", $"avg(count)").show(2)

    df.select(expr("*"), lit(1).as("One")).show(2)


  }
}
