package saied.spark
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{expr,not,col}
object Day7 {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("day7")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val data_dir = "/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv"
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_dir)

    df.show(5)
    df.printSchema()

    df.createOrReplaceTempView("dfTable")

    df.select(lit(5),lit("five"), lit(5.0)).show()

    df.where($"InvoiceNo".equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5)

    df.where("InvoiceNo = 536365").show(5, false)
    df.where("InvoiceNo <> 536365").show(5, false)
    df.where($"InvoiceNo" =!= 536365).show(5)
    // specify Boolean expressions serially
    val priceFilter = col("UnitPrice") >600
    val descriptFilter = col("Description").contains("POSTAGE")

    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptFilter))
      .show()

    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter1 = col("UnitPrice")>600
    val descriptfilter = col("Description").contains("POSTAGE")

    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter1.or(descriptfilter)))
      .show()

    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("InvoiceDate", "UnitPrice")
      .show(5)



  }
}
