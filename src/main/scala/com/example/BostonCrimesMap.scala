package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.control.Breaks
import org.apache.spark.sql.functions.broadcast


object BostonCrimesMap extends App {
  val spark = SparkSession.builder().appName("BostonCrimesMap").master("local[*]").getOrCreate()
  import spark.implicits._

  val crimeFacts = spark.read.format("csv").option("header", "true").load(args(0))
  val offenseCodes = spark.read.format("csv").option("header","true").load(args(1))
  crimeFacts.createOrReplaceTempView("crimeFactsTable")
  offenseCodes.createOrReplaceTempView("offenseCodesTable")
  var loop = new Breaks;

  //--------------------------CRIMES TOTAL------------------------------

  val crimesTotal = crimeFacts.select($"DISTRICT").groupBy($"DISTRICT").count()
  crimesTotal.repartition(1).write.parquet(args(2) + "\\crimes_total.parquet")

  //---------------------------CRIMES MONTHLY---------------------------

  val crimesMonthly = spark.sql("select DISTRICT, percentile_approx(MEDIAN, 0.5) as MONTH_MEDIAN_CRIMES FROM (select DISTRICT, MONTH, count(1) as MEDIAN FROM crimeFactsTable group by DISTRICT, MONTH) GROUP BY DISTRICT")
  crimesMonthly.repartition(1).write.parquet(args(2) + "\\crimes_monthly.parquet")

  //---------------------------FREQUENT CRIME TYPES----------------------

  val splitBySeparator = (crimeType: String) => {
    var result = ""
    loop.breakable {
      for (letter <- crimeType.split("")) {
        if (!(letter == "-")) {
          result += letter
        } else loop.break
      }
    }
    result.trim
  }
  spark.udf.register("splitBySeparator", splitBySeparator)
  val crimeTypeCode = spark.sql("select CODE, splitBySeparator(NAME) as NAME from offenseCodesTable")
  val offenseCodesBroadcast = broadcast(crimeTypeCode)
  val frequentCrimeTypes = offenseCodesBroadcast.join(crimeFacts, $"CODE" === $"OFFENSE_CODE").groupBy($"DISTRICT",$"NAME").count().orderBy($"count".desc)
  frequentCrimeTypes.repartition(1).write.parquet(args(2) + "\\frequent_crime_types.parquet")

  //-------------------------------LAT------------------------------------

  crimeFacts.groupBy($"DISTRICT")
    .agg((sum($"Lat")/count($"Lat")).as("avgLat"))
    .repartition(1).write.parquet(args(2) + "\\lat.parquet")

  //-------------------------------LONG-----------------------------------

  crimeFacts.groupBy($"DISTRICT")
    .agg((sum($"Long")/count($"Long")).as("avgLong"))
    .repartition(1).write.parquet(args(2) + "\\lng.parquet")
}
