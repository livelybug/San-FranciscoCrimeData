
import org.apache.log4j._
import org.apache.spark.sql._

import scala.util.Try
import scalax.file.Path
import org.apache.spark.ml.clustering.KMeans
/**
  * Created by Burt on 10/26/2016.
  */

object SFC_ML_Analyse{

  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter

  case class CriRecord(year:Int, month:String, dayOfWeek:String, district:String, x:Double, y:Double)

  def mapper(line:Row): CriRecord = {
    val dt =LocalDateTime.parse(line(1).toString, DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss"))

    CriRecord(
      dt.getYear,
      dt.getMonth.getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.UK),
      dt.getDayOfWeek.getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.UK),
      line(3).toString,
      Try(line(5).toString.toDouble) getOrElse(0.0),
      Try(line(6).toString.toDouble) getOrElse(0.0)
    )
  }


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val basePath = getClass.getResource(".").getPath
    val dirIn1 = "input/"
    val fileInPath1 = basePath + dirIn1 + "test.csv"

    val dirOut1 = "output/"
    // Delete files generated previously
    val outPath1: Path = Path (basePath + dirOut1)
    outPath1.deleteRecursively(true)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
        .config("spark.network.timeout", "500s")
      .getOrCreate()

    import spark.implicits._
    val text = spark.sparkContext.textFile(fileInPath1)
    println("---------" + text.first())

    val tStart = System.currentTimeMillis()
/*
    //Before Spark 2.0, use the line below
    val rows = text.mapPartitionsWithIndex{ (idx, iter) => if (idx == 0) iter.drop(1) else iter }
*/
    val dfRows = spark.read.option("header", true).csv(fileInPath1)
    println("---------" + dfRows.first())

    //val criDsInclude2015 = dfRows.map(mapper)
    //val criDs = criDsInclude2015.filter($"year" !== 2015).cache()
    val criDs = dfRows.map(mapper)
    println("---------" + criDs.show(2))

    import org.apache.spark.sql.functions._

    val feature_data_geo = criDs.select($"x", $"y")

    import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
    import org.apache.spark.ml.linalg.Vectors

    val assembler = new VectorAssembler().setInputCols(Array("x", "y")).setOutputCol("features")
    val training_data = assembler.transform(feature_data_geo).select("features")
    val kmeans = new KMeans().setK(12).setSeed(1L)
    val model = kmeans.fit(training_data)
    val WSSSE = model.computeCost(training_data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val tEnd = System.currentTimeMillis()
    println("------------" + "time elapsed: " + (tEnd - tStart) + " millisec")

    spark.stop()
  }
}
