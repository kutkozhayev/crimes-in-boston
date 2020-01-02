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


  val crimesTotal = crimeFacts.select("DISTRICT").groupBy("DISTRICT").count() // CRIMES TOTAL
  crimesTotal.repartition(1).write.parquet(args(2) + "\\crimes_total.parquet")

  val crimesMonthly = spark.sql("SELECT DISTRICT, MONTH, percentile_approx(MEDIAN, 0.5) AS MEDIAN FROM (select DISTRICT, MONTH, count(MONTH) AS MEDIAN FROM crimeFactsTable group by DISTRICT, MONTH) GROUP BY DISTRICT, MONTH")
  crimesMonthly.repartition(1).write.parquet(args(2) + "\\crimes_monthly.parquet")

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

  crimeFacts.groupBy($"DISTRICT")
    .agg((sum($"Lat")/count($"Lat")).as("avgLat"))
    .repartition(1).write.parquet(args(2) + "\\lat.parquet")

  crimeFacts.groupBy($"DISTRICT")
    .agg((sum($"Long")/count($"Long")).as("avgLong"))
    .repartition(1).write.parquet(args(2) + "\\lng.parquet")


   /*crimeTotal.repartition(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("mydata.csv")*/





  //crime.select($"DISTRICT").groupBy($"DISTRICT").sum("Lat").show()

  //crime.join(crimeType, $"CODE" === $"OFFENSE_CODE")
  //crime.select("MONTH").groupBy("MONTH").count().show()

  //spark.sql("select DISTRICT, sum(Lat)/count(Lat) as AvgLat from BostonTable group by DISTRICT").show()
  //spark.sql("select DISTRICT, sum(Long)/count(Long) as AvgLong from BostonTable group by DISTRICT").show()


}
