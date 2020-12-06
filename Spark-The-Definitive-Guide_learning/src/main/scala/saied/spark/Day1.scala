package saied.spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
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


//    filghtData2015.show(5)
    filghtData2015.sort("count").explain()



  }
}
