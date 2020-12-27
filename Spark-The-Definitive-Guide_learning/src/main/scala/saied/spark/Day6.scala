package saied.spark
import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.log4j._
import org.apache.spark.sql.functions.{asc, desc}
object Day6 {
  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val data_dir = "/home/saied/Data_Science/bIG_dATA/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json"
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Day6")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json").load(data_dir)
//    df.show(5)
//    df.printSchema()
//    println("shape of dataframe: "+(df.count(),df.columns.length))
//
//    df.withColumn("sth", lit("sth")).show(5)
//
//    df.withColumn("WithinCountry", expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME"))
//      .show(3)
//
//    println(df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns)
//    df.withColumn("sth", $"DEST_COUNTRY_NAME").show(3)

//    val dfWithLongColName = df.withColumn("this long-name", expr("DEST_COUNTRY_NAME"))
//
//    dfWithLongColName.selectExpr("`this long-name`", "`this long-name` as `long one`")
//      .show(2)
//
//    df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(2)
//
//    df.withColumn("count2", col("count").cast("long"))
//    df.withColumn("count2", $"count".cast("int")).printSchema()

//    df.filter(col("count")<2).show(2)
//    df.filter($"count" <3).show(2)
//    df.where("count < 4").show(2)
//
//    // using multiple where function instead of AND OR ...
//    df.where($"count"<10).where($"ORIGIN_COUNTRY_NAME" =!= "Croatia")
//      .where($"DEST_COUNTRY_NAME" =!= "United States").show(2)

//    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").distinct().show(2)
//    df.select("ORIGIN_COUNTRY_NAME").distinct().show(2)
//
//    println(df.select("DEST_COUNTRY_NAME").distinct().count())
//    println(df.distinct().count())
//    df.distinct().show(5)

    val seed = 42
    val WithReplacement = false
    val fraction = .5
//    println(df.sample(WithReplacement, fraction, seed).count())
//    df.sample(WithReplacement, fraction, seed).show(3)

//    val dataframes = df.randomSplit(Array(.25,.75), seed)
//    println("original shape of dataframe: "+(df.count(),df.columns.length))
//    println("1st shape of dataframe: "+(dataframes(0).count(),dataframes(0).columns.length))
//    println("second shape of dataframe: "+(dataframes(1).count(),dataframes(1).columns.length))


//    val schema = df.schema
//    val newRows = Seq(
//      Row("New Country", "Other Country", 5L),
//      Row("New Country 2", "Other Country 3", 1L)
//    )
//    val parallelizedRows = spark.sparkContext.parallelize(newRows)
//    val newDF = spark.createDataFrame(parallelizedRows, schema)
//    df.union(newDF)
//      .where("count = 1")
//      .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
//      .show()

    df.sort(expr("count asc")).show()
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show()


    
  }
}
