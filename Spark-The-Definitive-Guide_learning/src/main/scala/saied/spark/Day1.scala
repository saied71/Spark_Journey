package saied.spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.desc
object Day1 {
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val data_dir = "/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv"
    val spark = SparkSession
      .builder()
      .appName("day1")
      .master("local[*]")
      .getOrCreate()

    val filghtData2015 = spark
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv(data_dir)


    filghtData2015.show(5)
    filghtData2015.sort("count").explain()

    spark.conf.set("spark.sql.shuffle.partitions", 5)
    filghtData2015.createOrReplaceTempView("flight_data_2015")

    val sqlWay = spark.sql("""
       SELECT DEST_COUNTRY_NAME, count(1)
       FROM flight_data_2015
       GROUP BY DEST_COUNTRY_NAME
       """)

    val dataFrameWay = filghtData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()

    sqlWay.explain()
    dataFrameWay.explain()

    spark.sql("SELECT max(count) from flight_data_2015").take(1)

    filghtData2015.select(max("count")).take(1)

    val maxSql = spark.sql(
      """
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
        """)
    maxSql.show()

    filghtData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

    filghtData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .explain()


  }
}
