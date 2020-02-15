package com.example

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast

import scala.collection.immutable.ListMap
import scala.collection.mutable.HashMap


object BostonCrimesMap extends App {
  val spark = SparkSession.builder().appName("BostonCrimesMap").master("local[*]").getOrCreate()
  import spark.implicits._

  val crimeFacts = spark.read.format("csv").option("header", "true").load(args(0))
  val offenseCodes = spark.read.format("csv").option("header","true").load(args(1))
  crimeFacts.createOrReplaceTempView("crimeFactsTable")
  offenseCodes.createOrReplaceTempView("offenseCodesTable")

  //--------------------------CRIMES TOTAL------------------------------

  val crimesTotal = crimeFacts.groupBy($"DISTRICT").agg(count($"DISTRICT").as("totalCrimeCounts"))

  //---------------------------CRIMES MONTHLY---------------------------

  val crimesMonthly = spark.sql("select DISTRICT, YEAR, MONTH, percentile_approx(count(1),0.5) OVER (PARTITION BY DISTRICT) as MONTH_MEDIAN_CRIMES FROM crimeFactsTable group by DISTRICT, MONTH, YEAR order by DISTRICT, YEAR, MONTH")

  //---------------------------FREQUENT CRIME TYPES----------------------

  val crimeTypeCode = offenseCodes.select($"CODE", split($"NAME","-").getItem(0) as "NAME")
  val offenseCodesBroadcast = broadcast(crimeTypeCode)
  val frequentCrimeTypes = offenseCodesBroadcast.join(crimeFacts, $"CODE" === $"OFFENSE_CODE")
    .groupBy($"DISTRICT", $"NAME").agg(count($"NAME").as("CRIMENUMBER"))
    .withColumn("RANK", row_number().over(Window.partitionBy($"DISTRICT").orderBy($"CRIMENUMBER".desc))).where($"RANK" < 4)
    .groupBy($"DISTRICT").agg(concat_ws(", ", collect_list($"NAME")).as("FrequentCrimeTypes"))

  //-------------------------------LAT------------------------------------

  val avgLat = crimeFacts.groupBy($"DISTRICT")
    .agg((sum($"Lat")/count($"Lat")).as("avgLat"))

  //-------------------------------LONG-----------------------------------

  val avgLong = crimeFacts.groupBy($"DISTRICT")
    .agg((sum($"Long")/count($"Long")).as("avgLong"))


  crimesTotal.join(crimesMonthly, "DISTRICT")
    .join(frequentCrimeTypes, "DISTRICT")
    .join(avgLat, "DISTRICT")
    .join(avgLong, "DISTRICT")
    .coalesce(1).write.format("parquet").mode("append").save(args(2) + File.separator + "Boston crime analytics")
}


