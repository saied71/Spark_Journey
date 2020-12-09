package saied.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans
object ml_intro {
  def main(args:Array[String]): Unit ={



    // Optional: Use the following code below to set the Error reporting
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a Spark Session Instance
    val spark = SparkSession
      .builder()
      .appName("day1")
      .master("local[*]")
      .getOrCreate()

    // Import Kmeans clustering Algorithm

    val dir = "/home/saied/Data_Science/bIG_dATA/Scala-and-Spark-Bootcamp-master/Machine_Learning_Sections/Clustering/Wholesale_customers_data.csv"
    // Load the Wholesale Customers Data
    val dataset = spark.read.option("header","true").option("inferSchema","true").csv(dir)
    dataset.show(5)
    // Select the following columns for the training set:
    // Fresh, Milk, Grocery, Frozen, Detergents_Paper, Delicassen
    // Cal this new subset feature_data
    import spark.implicits._
    val feature_data = dataset.select($"Fresh", $"Milk", $"Grocery", $"Frozen", $"Detergents_Paper", $"Delicassen")


    // Import VectorAssembler and Vectors


    // Create a new VectorAssembler object called assembler for the feature
    // columns as the input Set the output column to be called features
    // Remember there is no Label column
    val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")).setOutputCol("features")

    // Use the assembler object to transform the feature_data
    // Call this new data training_data
    val training_data = assembler.transform(feature_data).select("features")

    // Create a Kmeans Model with K=3
    val kmeans = new KMeans().setK(3).setSeed(1L)

    // Fit that model to the training_data
    val model = kmeans.fit(training_data)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.summary.trainingCost
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
