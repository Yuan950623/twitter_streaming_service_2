import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("streaming_app").setMaster("local[*]").set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputStream = KafkaUtil.getKafkaStream("twitter", ssc)

    inputStream.foreachRDD {rdd =>
      println(rdd.map(_.value()).collect().mkString("\n"))
    }

    ssc.start()

    ssc.awaitTermination()

  }

}
