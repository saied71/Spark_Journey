package saied.spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions.{coalesce, col, column, date_format, desc, window}
import org.apache.spark.ml.feature.StringIndexer


case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)
object Day2 {
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Flight")
      .getOrCreate()
    import spark.implicits._

    val flightsDF = spark.read
      .parquet("/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/flight-data/parquet/2010-summary.parquet/")
    val flights = flightsDF.as[Flight]

    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .show(5)

    flights
      .take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
      .foreach(println)



    val staticDataFrame = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema", "true")
      .load("/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

    staticDataFrame.show(5)
    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema
    staticSchema.printTreeString()

    staticDataFrame.selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
      .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
      .sum("total_cost")
      .show(5)



  }

}
