import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkNasa1995Logs")
      .getOrCreate();

    val logs = spark.read.option("sep", " ").option("header", true).option("inferSchema", true).csv("src/main/resources/logs.csv")
  }
}