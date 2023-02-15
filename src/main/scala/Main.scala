import org.apache.spark.sql.functions.{col, hour, regexp_extract, to_date, trim, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkNasa1995Logs")
      .getOrCreate();

    //https://data.world/shad/nasa-website-data
    val logsRaw = spark.read.option("sep", " ").option("header", true).option("inferSchema", true).csv("src/main/resources/nasa_aug95.csv")

    val methods = """^GET |^POST |^HEAD """
    val protocols = """HTTP/.*$"""

    val logs = logsRaw.
      withColumn("method", regexp_extract(col("request"), methods, 0)).
      withColumn("protocol", trim(regexp_extract(col("request"),protocols, 0))).
      withColumn("resource",
        when(col("protocol").equalTo(""),
          regexp_extract(col("request"),"(?:" + methods + ")?(.*)", 1)).otherwise(
          regexp_extract(col("request"),"(?:" + methods + ")?(.*)(?:" + protocols + ")", 1))).
      select(col("requesting_host").as("host"), col("datetime"), col("method"), col("resource"), col("protocol"), col("status"),col("response_size").as("size"))

//    logs.show(false);
//    logs.where(col("method").equalTo("")).show(false);
//    logs.where(col("protocol").equalTo("")).show(false);

    query1(logs)
    query2(logs)
    query3(logs)
    query4(logs)
    query5(logs)
    query6(logs)
    query7(logs)
    query8(logs)
    query9(logs)
  }

  /**
   * ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
   */
  def query1(logs: DataFrame): Unit = {
    logs.select("protocol").distinct().show(false)
  }

  /**
   * ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
   * para ver cuál es el más común.
   */
  def query2(logs: DataFrame): Unit = {
    logs.groupBy("status").count().show(false)
  }

  /**
   * ¿Y los métodos de petición (verbos) más utilizados?
   */
  def query3(logs: DataFrame): Unit = {
    logs.groupBy("method").count().show(false)
  }
  /**
   * ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
   */
  def query4(logs: DataFrame): Unit = {
    logs.select("resource", "size").distinct().orderBy(col("size").desc).show(1, false)
  }

  /**
   * Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
   * decir, el recurso con más registros en nuestro log.
   */
  def query5(logs: DataFrame): Unit = {
    logs.groupBy("resource").count().orderBy(col("count").desc).show(1, false)
  }

  /**
   * ¿Qué días la web recibió más tráfico?
   */
  def query6(logs: DataFrame): Unit = {
    logs.withColumn("date", to_date(col("datetime"))).groupBy("date").count().orderBy(col("count").desc).show(false)
  }

  /**
   * ¿Cuáles son los hosts son los más frecuentes?
   */
  def query7(logs: DataFrame): Unit = {
    logs.groupBy("host").count().orderBy(col("count").desc).show(false)
  }

  /**
   * ¿A qué horas se produce el mayor número de tráfico en la web?
   */
  def query8(logs: DataFrame): Unit = {
    logs.withColumn("hour", hour(col("datetime"))).groupBy("hour").count().orderBy(col("count").desc).show(false)
  }

  /**
   * ¿Cuál es el número de errores 404 que ha habido cada día?
   */
  def query9(logs: DataFrame): Unit = {
    logs.withColumn("date", to_date(col("datetime"))).where(col("status").equalTo("404")).groupBy("date").count().orderBy(col("count").desc).show(false)
  }
}