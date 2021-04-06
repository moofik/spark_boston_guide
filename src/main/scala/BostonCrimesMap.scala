import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.broadcast

object BostonCrimesMap extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .config("spark.sql.autoBroadcastJoinThreshold", 0)
    .master("local[*]")
    .getOrCreate()

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/alexander/IdeaProjects/untitled/src/main/scala/crime.csv")

  val offenseCodes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/alexander/IdeaProjects/untitled/src/main/scala/offense_codes.csv")

  val offenseCodesBroadcast = broadcast(offenseCodes)

  val windowSpec = Window
    .partitionBy("YEAR", "MONTH", "DISTRICT", "OFFENSE_CODE")
    .orderBy("YEAR", "MONTH")

  def limitSize(n: Int, arrCol: Column): Column =
    array((0 until n).map(arrCol.getItem): _*)

  import spark.implicits._

  val frequentCrimesDF = crimeFacts
    .withColumn("max_count", count("OFFENSE_CODE").over(windowSpec))
    .groupBy("YEAR", "MONTH", "DISTRICT", "OFFENSE_CODE")
    .agg(max("max_count").as("FREQUENCY"))
    .select($"YEAR", $"MONTH", $"DISTRICT", $"FREQUENCY", $"OFFENSE_CODE".as("CODE"))
    .join(offenseCodesBroadcast, Seq("CODE"))
    .distinct()
    .orderBy($"YEAR".asc, $"MONTH".asc, $"DISTRICT".desc, $"FREQUENCY".desc)
    .select($"YEAR", $"MONTH", $"DISTRICT", $"FREQUENCY", trim(substring_index($"NAME", "-", 1)).as("CRIME_NAME"))
    .toDF()

  frequentCrimesDF.show()

  frequentCrimesDF
    .groupBy("YEAR", "MONTH", "DISTRICT")
    .agg(collect_list("CRIME_NAME").as("frequent_crimes"))
    .orderBy($"YEAR".asc, $"MONTH".asc, $"DISTRICT".desc)
    .select($"YEAR", $"MONTH", $"DISTRICT", limitSize(3, $"frequent_crimes").as("frequent_crime_types"))
    .show();
}
