package org.example

import com.typesafe.scalalogging.LazyLogging
import sttp.client4.quick._
import sttp.client4.Response
import upickle.core.LinkedHashMap
import upickle.default._
import ujson._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import os.Path
import sttp.model.Uri

import scala.+:

private class OpenMeteoClient(url: Uri = uri"https://api.open-meteo.com/v1/forecast") extends LazyLogging {
  /*

  */

  logger.info("Class OpenMeteoClient successfully created")

  private val urlURI = url.copy(querySegments = Nil, fragmentSegment = None)
  private val queryParams = url.paramsMap

  logger.info(s"Successfully create OpenMeteoClient object: url = ${urlURI.toString()} and params = $queryParams")


  def fetchForcast(params: Map[String, String]): String = {
    try {
      logger.info(s"Fetching request via params: ${queryParams ++ params}")
      val json = quickRequest.get(urlURI.addParams(queryParams ++ params)).send().body
      logger.info(s"Response body in JSON string format: \n $json")
      json
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get response for ", e)
        throw e
    }
  }

  def fetchForcast(params: Map[String, String], path: os.Path): os.Path = {
    val jsonObj = ujson.read(quickRequest.get(urlURI.addParams(queryParams++params)).send().body)

    os.write.over(path, jsonObj)
    path
  }
}

case class OpenMeteoView(val data: DataFrame, val units: DataFrame, spark: SparkSession) extends LazyLogging {
  /*
  класс объектов, здесь будут реализованы методы связанные с units (их нормализация, измененя типа "time" и т.д.)+
  */
  import spark.implicits._

  logger.info(s"Created OpenMeteView object via: \n Data: ${data.columns.mkString(", ")} \n Units: ${units.columns.mkString(", ")}")
  private lazy val unitsRow = units.first()

  def mergeFieldNames(): OpenMeteoView = {
    val newData = data.select(
      data.schema.fieldNames.map{ name =>
        val unit = unitsRow.getAs[String](name)
        col(name).as(s"${name}_$unit")
      }:_*
    )

    copy(newData)
  }

  def show(n: Int = 5): Unit = {
    data.show(n)
    units.show()
  }

  private def transform(cols: Seq[String], patterns: Map[String, (Column => Column, String)]): OpenMeteoView = {

    logger.info(s"Starting transformation (${patterns.map{ case (key, (_, to)) => ("from " + key, "to " + to)}.mkString(",")} ) for columns: ${cols.mkString(",")}")

    val checkCols = cols.exists(c => patterns.contains(unitsRow.getAs[String](c)))

    if (cols.isEmpty && !checkCols) {
      logger.warn("Column list is empty or contains already transformed ones. No transformation applied!")
      copy()
    }

    try {
      val newData = data.select(data.columns.map { name =>
        val currentUnit = if (unitsRow.schema.fieldNames.contains(name)) unitsRow.getAs[String](name) else ""
        (cols.contains(name), patterns.get(currentUnit)) match {
          case (true, Some((convertFn, _))) => convertFn(col(name)).as(name)
          case _ => col(name)
        }
      }: _*)

      val newUnits = units.select(units.columns.map { name =>
        val currentUnit = if (unitsRow.schema.fieldNames.contains(name)) unitsRow.getAs[String](name) else ""
        (cols.contains(name), patterns.get(currentUnit)) match {
          case (true, Some((_, toUnit))) => lit(toUnit).as(name)
          case _ => col(name)
        }
      }: _*)

      copy(newData, newUnits)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to transform ${patterns.keys.mkString(", ")}: ${e.getMessage}", e)
        throw e
    }
  }


//  def toTimestamp(cols: Seq[String] = Seq("time"), ogFormat: String = "unixtime", toFormat: String = "ISO8601"): OpenMeteoView = {
//
//    val checkCols = cols.exists{ c =>
//      val currentFormat = unitsRow.getAs[String](0)
//      currentFormat == ogFormat
//    }
//
//    if (!checkCols) {
//      println("Никаких изменений не произошло: либо все колонки приведены в необходимый формат, либо в списке колонок присутствуют колонки в требуемом формате")
//      this
//    } else {
//      // Переменная (DataFrame) с обновленными стоблцами в требуемом формате timestamp
//      val omvDataTS = data.select(
//        data.columns.map(name => if (cols.contains(name)) from_unixtime(col(name), OpenMeteo.Formats.getOrElse(toFormat, "yyyy-MM-dd HH:mm:ss"))
//        else col(name)): _*)
//      // Переменная (DataFrame) с обнвленными столбцами форматов/единиц измерений
//      val omvUnitsTS = cols.foldLeft(units) {(units, colName) =>
//        units.withColumn(colName,
//          when(col(colName)===ogFormat, lit(s"timestamp_$toFormat"))
//          .otherwise(col(colName)))
//      }
//      // Обновление класса
//      copy(omvDataTS, omvUnitsTS)
//    }
//  }

  def toTimestamp(cols: Seq[String], toFormat: String = "ISO8601"): OpenMeteoView = {
    val strFormat = OpenMeteo.Formats.getOrElse(toFormat, "yyyy-MM-dd HH:mm:ss")

    val tsPattern: Map[String, (Column => Column, String)] = Map(
      "unixtime" -> (c => from_unixtime(c, strFormat), s"timestamp_$toFormat"),
      "timestamp_date" -> (c => date_format(c, strFormat), s"timestamp_$toFormat"),
      "timestamp_ISO8601" -> (c => date_format(c, strFormat), s"timestamp_$toFormat")
    )

    transform(cols, tsPattern)
  }

  def toHours(cols: Seq[String]) = {
    val timePatterns: Map[String, (Column => Column, String)] = Map(
      "ms" -> (c => round(c.cast(DoubleType)/36000.0, 2), "hours"),
      "s" -> (c => round(c.cast(DoubleType)/3600.0, 2), "hours"),
      "m" -> (c => round(c.cast(DoubleType)/60.0, 2), "hours")
    )

    transform(cols, timePatterns)
  }

  def toCelsius(cols: Seq[String]) = {
    val tempPatterns: Map[String, (Column => Column, String)] = Map(
      "°F" -> (c => round((c.cast(DoubleType)-32.0)/ 1.8, 2), "celsius"),
      "K" -> (c => round(c.cast(DoubleType)-273.15, 2), "celsius")
    )

    transform(cols, tempPatterns)
  }

  def toMillimeters(cols: Seq[String]) = {
    val precipPatterns: Map[String, (Column => Column, String)] = Map(
      "inch" -> (c => round(c.cast(DoubleType)*25.4, 2), "mm"),
      "ft" -> (c => round(c.cast(DoubleType)*304.8, 2), "mm")
    )

    transform(cols, precipPatterns)
  }

  def toMetrPerSec(cols: Seq[String]) = {
    val speedPatterns: Map[String, (Column => Column, String)] = Map(
      "kn" -> (c => round(c.cast(DoubleType)*0.5144, 2), "m_per_s"),
      "km_per_s" -> (c => round(c.cast(DoubleType)*0.2778, 2), "m_per_s")
    )

    transform(cols, speedPatterns)
  }

  /* Так описывались функции до этого
  def toHours(cols: Seq[String], toUnit: String = "H"): OpenMeteoView = {
    val convertPattern = Map(
      "ms" -> 3600.0,
      "s" -> 3600.0,
      "m" -> 60.0
    )

    val unitsRow = units.first()
    val checkCols = cols.exists(c =>
      convertPattern.contains(unitsRow.getAs[String](c))
    )

    if (!checkCols) {
      println("Никаких изменений не произошло: либо все колонки приведены в необходимый формат, либо в списке колонок присутствуют колонки в требуемом формате")
      this
    }
    else {
      // Переменная (DataFrame) с обновленными стоблцами в требуемом формате timestamp
      val omvDataTS = this.data.select(
        this.data.columns.map{name =>
          val currentUnit = unitsRow.getAs[String](name)
          if (cols.contains(name) && convertPattern.contains(currentUnit)) (col(name)+convertPattern(currentUnit)).as(name)
          else col(name)}: _*)

      // Переменная (DataFrame) с обнвленными столбцами форматов/единиц измерений
      val omvUnitsTS = units.select(units.columns.map{ name =>
        val currentUnit = unitsRow.getAs[String](name)
        if (cols.contains(name) && convertPattern.contains(currentUnit)) lit(toUnit).as(name)
        else col(name)
      }:_*)
      // Обновление класса
      OpenMeteoView(omvDataTS, omvUnitsTS, spark)
    }
  }
  */

//  def aggregate(cols: Seq[String], timeCol: Seq[String],
//                ops: Map[String, (String, String, String) => Column],
//                suffix: String): OpenMeteoView = {
//
//    logger.info(s"Starting aggregation (${ops.keys.mkString(", ")}) for columns: ${cols.mkString(",")}")
//
//    val tempOMV = this.toTimestamp(timeCol, toFormat = "date")
//    val aggs: Seq[Column] = cols.flatMap { name =>
//      ops.map { case (key, op) =>
//        op(name, key, suffix)
//      }
//    }
//
//    try {
//      val newData = tempOMV.data
//        .groupBy(timeCol.map(col): _*)
//        .agg(aggs.head, aggs.tail: _*)
//
//      val newUnits = tempOMV.units.select((cols ++ timeCol).map(name => col(name)): _*)
//
//      copy(newData, newUnits)
//    } catch {
//      case e: Exception =>
//        logger.error(s"Failed to aggregate to ${ops.keys.mkString(",")}", e)
//        throw e
//    }
//  }

  private def aggregate(cols: Seq[String], keyCol: Seq[String],
                        op: Seq[String],
                        suffix: String) ={

    logger.info(s"Starting aggregation (${op.mkString(", ")}) for columns: ${cols.mkString(",")}")

    val aggFn = op.map(name =>
      (name, OpenMeteo.Ops
      .getOrElse(name, (n:String, _: String, _: String) => first(lit(null)).as(s"failed_$n"))
    ))
    val aggs: Seq[Column] = cols.flatMap { name =>
      aggFn.map{ case(opr, ag) => ag(name, opr, suffix)}
    }

    try {
      val newData = data
        .groupBy(keyCol.map(col): _*)
        .agg(aggs.head, aggs.tail: _*)

      val unitsRow = units.first()
      val newUnits = units.select(
        keyCol.map(name => col(name)) ++
          cols.flatMap { c =>
            if (unitsRow.schema.fieldNames.contains(c)) {
              val ogUnit = unitsRow.getAs[String](c)
              op.map(name => lit(ogUnit).as(s"${name}_${c}_$suffix"))
            }
            else Seq.empty[Column]
          }: _*)

      copy(newData, newUnits)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to aggregate to ${op.mkString(",")}", e)
        throw e
    }
  }

  def agg24H(cols: Seq[String], timeCol: Seq[String] = Seq("time"), op: Seq[String]): OpenMeteoView = {
    val tempOMV = this.toTimestamp(timeCol, toFormat = "date")

    tempOMV.aggregate(cols, timeCol, op, "24h")
  }

  def aggDaylight(cols: Seq[String], timeCol: Seq[String] = Seq("time"), op: Seq[String]): OpenMeteoView = {
    val tempOMV = this.toTimestamp(timeCol, toFormat = "date")

    tempOMV.aggregate(cols, timeCol, op, "daylight")
  }

//  def avgDaylight(cols: Seq[String], timeCol: Seq[String] = Seq("time")): OpenMeteoView = {
//    val tempOMV = this.toTimestamp(timeCol, toFormat = "date")
//    val aggs: Seq[Column] = cols.map(name => round(avg(col(name)), 2).as(s"avg_${name}_daylight"))
//
//    val newData = tempOMV.data
//      .groupBy(timeCol.map(col):_*)
//      .agg(aggs.head, aggs.tail:_*)
//
//    val newUnits = units.select((cols++timeCol).map(name => col(name)):_*)
//
//    OpenMeteoView(newData, newUnits, spark)
//  }

  def join(other: OpenMeteoView, on: Column, query: Seq[Column],
           leftAlias: String = "h", rightAlias: String = "d",
           how: String = "inner"): OpenMeteoView = {

    logger.info(s"Starting joining OpenMeteoViews $leftAlias and $rightAlias")

    try {
      val joinedData = data.as(leftAlias).join(other.data.as(rightAlias), on, how).select(query:_*)

      val joinedDataCols = joinedData.columns
      val joinedUnits = units.select(
        joinedDataCols.collect {
          case name if units.columns.contains(name) => col(name)
          case name if other.unitsRow.schema.fieldNames.contains(name) =>
            val unit = other.unitsRow.getAs[String](name)
            lit(unit).as(name)
        }: _*
      )

      copy(joinedData, joinedUnits)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to join objects: ${e.getMessage} ", e)
        throw e
    }
  }

  def select(cols: Seq[String]): OpenMeteoView = {
    val newData = data.select(cols.map(col):_*)

    val newDataCols = newData.columns
    val newUnits = units.select(newDataCols.map(col):_*)

    copy(newData, newUnits)
  }

  def drop(cols: Seq[String]): OpenMeteoView = {
    logger.info(s"Dropping columns: ${cols.mkString(",")}")
    val newData = data.drop(cols:_*)

    val newUnits = units.drop(col(cols.head), cols.tail.map(col):_*)

    copy(newData, newUnits)
  }

}

object OpenMeteo extends LazyLogging {
  /*

  */

  val Formats: Map[String, String] = Map(
    "ISO8601" -> "yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX",
    "date" -> "yyyy-MM-dd"
  )

  val Ops: Map[String, (String, String, String) => Column] = Map(
    "avg" -> ((name, prefix, suffix) => round(avg(col(name).cast(DoubleType)), 2).as(s"${prefix}_${name}_$suffix")),
    "total" -> ((name, prefix, suffix) => round(sum(col(name).cast(DoubleType)), 2).as(s"${prefix}_${name}_$suffix")),
    "min" -> ((name, prefix, suffix) => round(min(col(name).cast(DoubleType)), 2).as(s"${prefix}_${name}_$suffix")),
    "max" -> ((name, prefix, suffix) => round(max(col(name).cast(DoubleType)), 2).as(s"${prefix}_${name}_$suffix"))
  )

  def toDF(json: String, spark: SparkSession): DataFrame = {
    logger.info(s"Starting to convert String in JSON-format to DataFrame")
    import spark.implicits._
    val df = spark.read.json(Seq(json).toDS())
    logger.info(s"Successfully converted JSON-format String to DataFrame: \n ${df.show(5)}")
    df
  }
  def toDF(json: os.Path, spark: SparkSession): DataFrame ={
    logger.info(s"Starting to convert JSON file (${json.toString()}) to DataFrame")
    spark.read.json(json.toString)
  }

  private def toInlineDF(df: DataFrame, tableName: String, spark: SparkSession): DataFrame = {
    logger.info(s"Starting to explode sub DataFrame (Nested structure): $tableName to DataFrame: \n ${df.show(5)}")
    df.select(
      col("utc_offset_seconds"),
      inline(
      arrays_zip(
        df.select(s"$tableName.*").columns
          .map(name => col(s"$tableName.$name").as(name)): _*
      )))
  }

  private def toBaseDF(df: DataFrame, tableName: String, spark: SparkSession): DataFrame = {
    df.select(s"$tableName.*")
  }

  private def toCsv(path: String, df: OpenMeteoView, spark: SparkSession): Unit = {
    logger.info(s"Starting to load DataFrame to CSV format at $path")
    val outputDF = df.mergeFieldNames().data

    outputDF.write.mode("overwrite").option("header", "true").csv(path)
    logger.info(s"DataFrame is successfully loaded to .csv format at $path")
  }

  private def toCsv(path: os.Path, df: OpenMeteoView, spark: SparkSession): Unit = {
    logger.info(s"Starting to load DataFrame to CSV format at ${path.toString()}")

    val outputDF = df.mergeFieldNames().data

    outputDF.write.mode("overwrite").option("header", "true").csv(path.toString())
    logger.info(s"DataFrame is successfully loaded to .csv format at ${path.toString()}")
  }

//  private def toDatabase(): Unit = {
//
//  }
}

case class OpenMeteo(df: DataFrame, spark: SparkSession) extends LazyLogging {

//  private val  nestedSections = df.schema.fields
//    .filter(_.dataType.isInstanceOf[StructType])
//    .map(_.name)
//  private val convDFs = nestedSections.map {
//    name => (name, df.select(s"$name.*"))
//  }.toMap
  logger.info(s"Created OpenMeteo object via DataFrame: \n ${df.schema.mkString("\n")}")
  // Эту часть еще обдумать, т.к. можно придумать более динамичную структуру через Map например
  private val utc_offset = col("utc_offset_seconds")
  private val tableName1: String = "hourly"
  private val tableName2: String = "daily"
  private val unitsTable1 = "hourly_units"
  private val unitsTable2 = "daily_units"

  logger.info(s"Creating DataFrames for hourly and daily OpenMeteoViews")

  private val hourlyDF = OpenMeteo.toInlineDF(df, tableName1, spark).withColumn("time", col("time")+utc_offset).drop(utc_offset)
  private val hourlyUnits = OpenMeteo.toBaseDF(df, unitsTable1, spark)
  private val dailyDF = OpenMeteo.toInlineDF(df, tableName2, spark).withColumn("time", col("time")+utc_offset).drop(utc_offset)
  private val dailyUnits = OpenMeteo.toBaseDF(df, unitsTable2, spark)

  val hourlyView: OpenMeteoView = OpenMeteoView(hourlyDF, hourlyUnits, spark)
  val dailyView: OpenMeteoView = OpenMeteoView(dailyDF, dailyUnits, spark)

  def runDefaultPipeline(
      tempCols: Seq[String],
      precipCols: Seq[String],
      speedCols: Seq[String],
      avgCols: Seq[String],
      totalCols: Seq[String],
      tsCols: Seq[String],
      timeCols: Seq[String],
      csvLoadPath: os.Path,
  ): Unit ={
    val hourlyReadable = hourlyView
      .toTimestamp(Seq("time"))
      .toMetrPerSec(speedCols)
      .toMillimeters(precipCols)
      .toCelsius(tempCols)

    val dailyReadable = dailyView
      .toTimestamp(tsCols)
      .toHours(timeCols)

    val hourlyDaylightView = hourlyReadable
      .join(dailyReadable, col("h.time").between(col("d.sunrise"), col("d.sunset")), Seq(col("h.*")))


    val hourlyViewAvg24H = hourlyView
      .agg24H(avgCols, op = Seq("avg"))
    val hourlyViewTotal24H = hourlyView
      .agg24H(totalCols, op = Seq("total"))


    val hourlyViewAvgDaylight = hourlyDaylightView
      .aggDaylight(avgCols, op = Seq("avg"))
    val hourlyViewTotalDaylight = hourlyDaylightView
      .aggDaylight(totalCols, op = Seq("total"))

    val hourlyViewAgg = hourlyViewAvg24H
      .join(hourlyViewTotal24H, col("avg24.time")===col("ttl24.time"), col("avg24.*") +: hourlyViewTotal24H.data.columns.filter(_ != "time").map(c => col(s"ttl24.$c")), "avg24", "ttl24")
//      .drop(Seq("ttl24.time"))
      .join(hourlyViewAvgDaylight, col("avg24.time")===col("avgDl.time"), col("avg24.*") +: hourlyViewAvgDaylight.data.columns.filter(_ != "time").map(c => col(s"avgDl.$c")), "avg24", "avgDl")
//      .drop(Seq("avgDl.time"))
      .join(hourlyViewTotalDaylight, col("avg24.time")===col("ttlDl.time"), col("avg24.*") +: hourlyViewTotalDaylight.data.columns.filter(_ != "time").map(c => col(s"ttlDl.$c")), "avg24", "ttlDl")
//      .drop(Seq("ttlDl.time"))
      .toTimestamp(Seq("time"))
      .join(dailyReadable, col("avg24.time")===col("d.time"), col("avg24.*") +: dailyReadable.data.columns.filter(_ != "time").map(c => col(s"d.$c")), "avg24", "d")
//      .drop(Seq("d.time"))

    println(s"ебанный hourlyViewAgg \n ${hourlyViewAgg.show()}")

    OpenMeteo.toCsv(csvLoadPath / "Open-Meteo-Hourly.csv", hourlyReadable.select("time" +: (tempCols ++ precipCols ++ speedCols)), spark)
    OpenMeteo.toCsv(csvLoadPath / "Open-Meteo-Daily.csv", dailyReadable, spark)
    OpenMeteo.toCsv(csvLoadPath / "Open-Meteo-Hourly-Aggregated.csv", hourlyViewAgg, spark)

  }


}
