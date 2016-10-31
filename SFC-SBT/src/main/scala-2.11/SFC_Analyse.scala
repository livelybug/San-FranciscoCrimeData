
//import SFC_Analyse.{getClass => _, _}
import java.util

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

import scalax.file.Path

/**
  * Created by Burt on 10/26/2016.
  */

object SFC_Analyse{

  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter
  case class CriRecord(year:Int, month:String, dayOfWeek:String, district:String)

  def mapper(line:Row): CriRecord = {
    val dt =LocalDateTime.parse(line(1).toString, DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss"))

    CriRecord(
      dt.getYear,
      dt.getMonth.getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.UK),
      dt.getDayOfWeek.getDisplayName(java.time.format.TextStyle.SHORT, java.util.Locale.UK),
      line(3).toString
    )
  }


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val basePath = getClass.getResource(".").getPath
    val dirIn1 = "input/"
    val fileInPath1 = basePath + dirIn1 + "test-small.csv"

    val dirOut1 = "output/"
    // Delete files generated previously
    val outPath1: Path = Path (basePath + dirOut1)
    outPath1.deleteRecursively(true)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
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

    val criDsInclude2015 = dfRows.map(mapper)
    val criDs = criDsInclude2015.filter($"year" !== 2015).cache()
    println("---------" + criDs.show(2))

    import org.apache.spark.sql.functions._

    /**
      * Calc crime number by year, save to a file
      */
    val outPathByYear = basePath + dirOut1 + "byYear"
    val byYear = criDs.groupBy("year").count().sort("year")
    byYear.repartition(1).write.option("header",true).csv(outPathByYear)

    /**
      * Calc crime number by month , save to a file
      */
    val outPathByMonth = basePath + dirOut1 + "byMonth"
    val byMonth = criDs.groupBy("month").count().sort(desc("count"))
    byMonth.repartition(1).write.option("header",true).csv(outPathByMonth)

    /**
      * Calc crime number by day of week, save  to a file
      */
    val outPathByDayOfWeek = basePath + dirOut1 + "byDayOfWeek"
    val byDayOfWeek = criDs.groupBy("dayOfWeek").count().sort(desc("count"))
    byDayOfWeek.repartition(1).write.option("header",true).csv(outPathByDayOfWeek)

    /**
      * Calc top 10 crime district, save  to a file
      */
    val outPathByDistrict = basePath + dirOut1 + "byDistrict"
    val byDistrict = criDs.groupBy("district").count().sort(desc("count")).limit(10).cache()
    byDistrict.repartition(1).write.option("header",true).csv(outPathByDistrict)

    /**
      * Calc top crime number of 4 crime district per year, save  to a file
      */
    val outPathByDistrictYear = basePath + dirOut1 + "byDistrictYear"
    val topAddNames = byDistrict.select("district").rdd.map(_(0).toString).take(4)
    //case class TopDistricts(year: Int, top1: Int, top2: Int, top3: Int, top4: Int)

    def composeQuery(colName: String): String = {
      var sqlQuery = ""
      val orOp = " or "
      for(disName <- topAddNames) { sqlQuery += s"$colName like '$disName'" + orOp }
      sqlQuery.stripSuffix(orOp)
    }

    val topDistrictYear = criDs.where(composeQuery("district"))
      .groupBy("year","district").count()
      .groupBy("year").pivot("district").sum("count").sort("year")

    topDistrictYear.repartition(1).write.option("header",true).csv(outPathByDistrictYear)

    val tEnd = System.currentTimeMillis()
    println("------------" + "time elapsed: " + (tEnd - tStart) + " millisec")

    spark.stop()
  }
}
