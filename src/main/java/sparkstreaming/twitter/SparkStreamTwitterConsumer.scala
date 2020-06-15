package sparkstreaming.twitter

import java.util.Date

import config.{KafkaConfigurations, MySQLConfig, SparkConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import utils.MySQLDatabaseFunction

object SparkStreamTwitterConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConfig= new SparkConfig
    val kafkaConfig= new KafkaConfigurations
    val mysqConfig= new MySQLConfig
    val topics="twitter-news"
    val topicSet=topics.split((",")).toSet
    val kafKaparms=Map[String,Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConfig.brokers,
          ConsumerConfig.GROUP_ID_CONFIG -> kafkaConfig.groupId,
           ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->classOf[StringDeserializer],
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      )

      /*
       Get data from Kafka topic
      */
      val messages= KafkaUtils.createDirectStream(sparkConfig.streamingContext,
        LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topicSet, kafKaparms));
      val lines =messages.map(_.value)

      import sparkConfig.sqlContext.implicits._
      var i = 0;
      lines.count().print()

    lines.foreachRDD(line=>{
        println("Number of twitter Received at "+new Date(System.currentTimeMillis()).toString+" : "+line.count())

        if(line.isEmpty()){
          println("No Data Received this time, please check if Tweets producer is running as Expected")
        }
        else {
          val dLine = line.filter(!_.isEmpty)
          if (dLine != null || !dLine.isEmpty()) {
            //write as json object
            dLine.toDF().write.mode("append").json("/Users/Olivier/Desktop/Hamubere/SparkExample/src/main/resources/data/twitter/")
            //write as text
            //line.toDF().write.mode("append").text("/Users/Olivier/Desktop/SparkStreamingExample/src/main/resources/data/output1")

          }
         val values= dLine.toDF().map(_.toString).map(_.replace("{", "")).
            map(_.replace("}", "")).map(_.split(","))
          values.show()
        }
       }
        )
      sparkConfig.streamingContext.start()
      sparkConfig.streamingContext.awaitTermination()



    }


}
