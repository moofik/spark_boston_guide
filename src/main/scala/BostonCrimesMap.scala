import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.broadcast

object BostonCrimesMap extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val (inputFile1, inputFile2, outputFile) = (args(0), args(1), args(2))

  val spark = SparkSession
    .builder()
    .config("spark.sql.autoBroadcastJoinThreshold", 0)
    .master("local[*]")
    .getOrCreate()

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputFile1)

  val offenseCodes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputFile2)

  val offenseCodesBroadcast = broadcast(offenseCodes)

  def limitSize(n: Int, arrCol: Column): Column =
    array((0 until n).map(arrCol.getItem): _*)

  import spark.implicits._

  val windowDistrictByAllTime = Window
    .partitionBy("DISTRICT", "OFFENSE_CODE")
    .orderBy("YEAR", "MONTH")

  var frequentCrimesDF = crimeFacts
    .withColumn("max_count", count("OFFENSE_CODE").over(windowDistrictByAllTime))
    .groupBy("DISTRICT", "OFFENSE_CODE")
    .agg(max("max_count").as("FREQUENCY"))
    .select($"DISTRICT", $"FREQUENCY", $"OFFENSE_CODE".as("CODE"))
    .join(offenseCodesBroadcast, Seq("CODE"))
    .distinct()
    .orderBy($"DISTRICT".desc, $"FREQUENCY".desc)
    .select($"DISTRICT", $"FREQUENCY", trim(substring_index($"NAME", "-", 1)).as("CRIME_NAME"))
    .toDF()

  frequentCrimesDF = frequentCrimesDF
    .groupBy("DISTRICT")
    .agg(collect_list("CRIME_NAME").as("frequent_crimes"))
    .orderBy($"DISTRICT".desc)
    .select($"DISTRICT", limitSize(3, $"frequent_crimes").as("frequent_crime_types"))

  crimeFacts
    .groupBy($"DISTRICT")
    .agg(
      count("*").as("crimes_total"),
      ceil(count("*") / 12).as("crimes_monthly"),
      avg("Lat").as("lat"),
      avg("Long").as("long")
    )
    .orderBy($"crimes_total".desc)
    .join(frequentCrimesDF, Seq("DISTRICT"))
    .distinct()
    .write
    .parquet(outputFile)

  spark
    .read
    .parquet(outputFile)
    .show(false)
}
