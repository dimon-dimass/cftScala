package org.example

import sttp.client4.quick._
import sttp.client4.Response
import upickle.core.LinkedHashMap
import upickle.default._
import ujson._
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments){
  val api = opt[String](required = false, default = Some("https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2026-02-12&end_date=2026-03-10"))
  val startDate = opt[String](required = false, default = Some("2026-02-12"))
  val endDate = opt[String](required = false, default = Some("2026-02-28"))

  verify()
}

object App {

  def main(args : Array[String]) {

    val conf = new Conf(args)

//    val url = "https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2026-01-16&end_date=2026-01-31"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("log4j.logger.org.apache.spark", "WARN")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("WARN")

    val openMeteoFetch = new OpenMeteoClient(uri"${conf.api()}")
    val params = if (args.isEmpty)
        Map.empty[String, String]
      else Map(
        "start_date" -> conf.startDate(),
        "end_date" -> conf.endDate()
      )
    val openMeteo = OpenMeteo(OpenMeteo.toDF(openMeteoFetch.fetchForcast(params),spark), spark)

    val tempCols = Seq(
      "temperature_2m",
      "dew_point_2m",
      "apparent_temperature",
      "temperature_80m",
      "temperature_120m",
      "soil_temperature_0cm",
      "soil_temperature_6cm"
    )
    val speedCols = Seq(
      "wind_speed_10m",
      "wind_speed_80m"
    )
    val tsCols = Seq(
      "time",
      "sunrise",
      "sunset"
    )
    val timeCols = Seq(
      "daylight_duration"
    )
    val precipCols = Seq(
      "rain",
      "showers",
      "snowfall"
    )
    val avgCols = tempCols.dropRight(2) ++ speedCols ++ Seq("relative_humidity_2m", "visibility")
    val totalCols = precipCols

    val csvLoadPath = os.pwd / "tmp"

    openMeteo.runDefaultPipeline(tempCols, precipCols, speedCols, avgCols, totalCols, tsCols, timeCols, csvLoadPath)
//    val response = quickRequest.get(uri).send()
//    val json_res = ujson.read(response.body)
//
//    val meteoMetrics = ujson.write(json_res.obj.filter { case (key,_) => Seq("hourly_units", "daily_units").contains(key) })
//    val hourlyJson = ujson.write(json_res("hourly"))
//    val dailyJson = ujson.write(json_res("daily"))
//    val seq = Seq(ujson.write(json_res))
//    println(dailyKeys)
//    println(dailyKeys.map(k => dailyJson(k).arr.map(_.num.toInt)(0)).toSeq)

//    val daily = dailyJson("time").arr.indices.map { i =>
//      val values = dailyKeys.map { k =>
//        val field = dailyJson(k).arr(i)
//        if (getType(field) > 0) field.num else field.str
//      }.toSeq
//      Row.fromSeq(values)
////      Row.fromSeq(dailyKeys.map(dailyJson(_).arr.map(_.num.toInt)(i)).toSeq)
//    }
//    val dailyRDD = spark.sparkContext.parallelize(daily)
//
//    val schema = StructType(Array(StructField("time", IntegerType, false),
//      StructField("sunrise", IntegerType, false),
//      StructField("sunset", IntegerType, false),
//      StructField("daylight_dur", IntegerType, false)))
//
//
//    val df = spark.createDataFrame(dailyRDD, schema)
//    val df = spark.createDataFrame(Seq(Row(1,2,3),Row(1,2,3)),schema=schema)
//
//    val ds = seq.toDS()
//
//    val df = spark.read.json(ds)
//    os.write(os.temp.dir(os.pwd, prefix = "tmp", deleteOnExit = true) / "raw-open-meteo.json", ujson.write(json_res))
//    os.write.over(os.pwd / "tmp" / "metrics.json", meteoMetrics)
//    os.write.over(os.pwd / "tmp" / "hourly.json", hourlyJson)
//    os.write.over(os.pwd / "tmp" / "daily.json", dailyJson)

//    val df = spark.read.json((os.pwd / "tmp" / "daily.json").toString())
//    val dfNorm = df.select(
//      inline(arrays_zip(
//        col("time"),
//        col("sunrise"),
//        col("sunset"),
//        col("daylight_duration")
//      ))
//    )
//    df.show(5)
//    df.select("hourly.*", "utc_offset_seconds")
//      .select(inline(arrays_zip(
//        df.select("hourly.*").columns.map(col): _*
//        ))).show(5)
//    val hourlyFields = df.select("hourly.*").columns
//    val colsToZip = hourlyFields.map(name => col(s"hourly.$name").as(name))
//    val hourlyDF = df.select($"utc_offset_seconds", inline(arrays_zip(
//        df.select("hourly.*").columns.map(name => col(s"hourly.$name").as(name)):_*
//      ))
//    )
//    val hourlyDFdate = hourlyDF.withColumn("time", from_unixtime(col("time")+col("utc_offset_seconds"), "yyyy-MM-dd"))
//
//    val ops: Map[String, (String, String, String) => Column] = Map(
//      "avg" -> ((name, prefix, suffix) => round(avg(col(name)), 2).as(s"${prefix}_${name}_$suffix")),
//      "total" -> ((name, prefix, suffix) => round(sum(col(name)), 2).as(s"${prefix}_${name}_$suffix")),
//      "count" -> ((name, prefix, suffix) => count(col(name)).as(s"${prefix}_${name}_$suffix"))
//    )
//
//    val op = Seq("avg","count")
//    val aggFn = op.map(name =>
//      (name, ops.getOrElse(name,
//        (n:String, _: String, _: String) => first(lit(null)).as(s"failed_$n"))
//      )
//    )
//    //    val aggFn = OpenMeteo.Ops.getOrElse(op, (name:String, _: String, _: String) => col(name))
//    val aggs: Seq[Column] = Seq("temperature_2m","dew_point_2m","visibility").flatMap { name =>
//      aggFn.map{ case(opr, fn) => fn(name, opr, "24h")}
//    }

//    hourlyDFdate.groupBy("time").agg(aggs.head, aggs.tail:_*).show(5)

//    hourlyDFdate.select(col("time"), col("temperature_2m")).where("""to_date(time) = "2026-01-15" """).show()

//    val dailyDF = df.select(col("utc_offset_seconds"), inline(arrays_zip(
//            df.select("daily.*").columns.map(name => col(s"daily.$name").as(name)): _*
//            )))
//    val unitsDF = df.select("hourly_units.*")
//    val dUnitsDF = df.select("daily_units.*")
//    val unitsRow = unitsDF.first()
//    val dUnitsRow = dUnitsDF.first()
//    val cols = Seq("temperature_2m", "dew_point_2m")

//    unitsDF.select(
//      Seq("time").map( name => col(name)) ++
//        Seq("temperature_2m","dew_point_2m","visibility").flatMap{ name =>
//          val ogUnit = if (unitsRow.schema.fieldNames.contains(name)) unitsRow.getAs[String](name) else ""
//
//          op.map{ opr =>
//            lit(ogUnit).as(s"${opr}_${name}_24h")
//          }
//        }:_*
//    ).show()
//    hourlyDF.show(10)
//    val joined = hourlyDF.as("h").join(dailyDF.as("d"), col("h.time") === col("d.time"))
//    val joinedCols = joined.columns
//    joined.show(5)
//
//    joined.drop(col("d.time"), col("d.sunset")).show(5)
//
//    val joinedUnits = unitsDF.select(
//      joinedCols.collect {
//        case name if unitsDF.columns.contains(name) => col(name)
//        case name if dUnitsRow.schema.fieldNames.contains(name) =>
//          val unit = dUnitsRow.getAs[String](name)
//          lit(unit).as(name)
//      }:_*
//    )

//    val newUnits = unitsDF.select(
//      Seq("time").map(name => col(name)) ++
//        cols.flatMap{ c =>
//          if(unitsRow.schema.fieldNames.contains(c)) {
//            val ogUnit = unitsRow.getAs[String](c)
//            op.map( name => lit(ogUnit).as(s"${name}_${c}_24h"))
//          }
//          else Seq.empty[Column]
//        }:_*)
//
//    newUnits.show()
//
//    joinedUnits.show()
//
//    unitsDF.show()
//    dailyDF.show(5)
//
//    val times = dailyDF.select(
//      col("time")
//      ,(col("time")+col("utc_offset_seconds")).as("time_off")
//      ,from_unixtime(col("time")).as("time_ts")
//      ,from_unixtime(col("time")+col("utc_offset_seconds")).as("time_offts")
//      ,from_unixtime(col("time")+col("utc_offset_seconds"), "yyyy-MM-dd").as("date"))
//
//    times.show(5)
//
//    val omvDataTS = dailyDF.select(
//      dailyDF.columns.map(name => if (cols.contains(name)) from_unixtime(col(name)).as(name)
//        else col(name)):_*)
//    Seq("time", "sunset").foldLeft(unitsDF) { (units, name) =>
//      units.withColumn(name,
//        when(col(name) === "unixtime", lit("timestampISO8601"))
//          .otherwise(col(name)))
//    }.show()
//    omvDataTS.show(5)
//    unitsDF.select(cols.map(name => from_unixtime(col(name)).as(name)):_*).show(5)
    //    print(cols.map(name => col(s"daily_units.$name").isin(List("unixtime"):_*)).mkString(" && "))
//    unitsDF.filter(cols.map(name => col(name)==="unixtime").reduce(_ && _)).show()
//    println(unitsDF.filter(cols.map(name => col(s"daily_units.$name")==="unixtime").reduce(_ && _)).isEmpty)

//    df.select("hourly.*","utc_offset_seconds").show()
//    df.select("hourly.*").withColumn("utc_offset_seconds",df.col("utc_offset_seconds")).show()
//    df.select("daily_units.*").show()
//    df.select(
//        inline(arrays_zip(
//          df.columns.map(c => col(c)): _*
//        )))
//      .show(5)
//    df.select("utc_offset_seconds").show()
//    dfNorm.show(5)
//    df.printSchema()
//    println(df.schema.fields.map(f => (f.name, f.dataType.getClass)).mkString("\n"))
//    println(df.schema.fields
//      .filter(_.dataType.isInstanceOf[StructType])
//      .map(_.name)
//      .map(name => (name, df.select(s"$name.*")))
//      .toMap)
//
//    println(df.schema.fields.filter(_.isInstanceOf[StructType]).map(_.name).mkString(", "))

//    df.select("daily.*").show(5)

//    spark.stop()

  }

}
