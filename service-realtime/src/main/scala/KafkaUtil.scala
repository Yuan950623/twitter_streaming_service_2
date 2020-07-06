import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaUtil {

  val properties = PropertyUtil.readProp("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  val kafkaConfig = Map (
    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(topic:String, ssc:StreamingContext) = {
    val dStream = KafkaUtils.createDirectStream[String,String](ssc,
                                                              LocationStrategies.PreferConsistent,
                                                              ConsumerStrategies.Subscribe[String, String](Array(topic),
                                                                kafkaConfig))

    dStream
  }


}
