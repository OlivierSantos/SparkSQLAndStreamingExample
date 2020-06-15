package sparkstreaming.wordCount

import config.{KafkaConfigurations, MySQLConfig, SparkConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import utils.MySQLDatabaseFunction

object SparkStreamingCountWord {

  def main(args: Array[String]): Unit ={

    println("--- Spark Streaming App Starting ----")
    val sparkConfig=new SparkConfig
    val kafkaConfig= new KafkaConfigurations
    val mysqConfig= new MySQLConfig
    val mysqlDatabaseFunction= new MySQLDatabaseFunction
    val topicSet=kafkaConfig.topics.split((",")).toSet
    val kafKaparms=Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->kafkaConfig.brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> kafkaConfig.groupId,
     // ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY->"org.apache.kafka.clients.consumer.RangeAssignor",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    /*
     Get data from Kafka
    */
    val messages= KafkaUtils.createDirectStream(sparkConfig.streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafKaparms));

    val line =messages.map(_.value)
    line.print()
    val words= line.flatMap(_.split(" "))

    words.print()
    val wordCounts= words.map(x=>(x.toLowerCase, 1L)).reduceByKey(_+_)
    print("Checking the value:")
    wordCounts.print()

    import sparkConfig.sqlContext.implicits._
 /*
    Write and retrieve data from mysql datbase and do some procession on the DataFrame
 */
    // //
     val properties = mysqlDatabaseFunction.setConnection()
    var dataFromDatabase = mysqlDatabaseFunction.readFromDatabase(sparkConfig.sqlContext
      ,mysqConfig.urlValue,mysqConfig.tableName,properties)

    wordCounts.foreachRDD( rdd => {
      if (rdd.count() > 0) {
        val df = rdd.toDF

        mysqlDatabaseFunction.writeToDatabase(sparkConfig.sqlContext,
          mysqConfig.urlValue,mysqConfig.tableName,df,properties)
        }
       dataFromDatabase =mysqlDatabaseFunction.readFromDatabase(sparkConfig.sqlContext,
         mysqConfig.urlValue,mysqConfig.tableName,properties)
      dataFromDatabase.show()


      }
    )

//Starting sparting streaming and set the awaittermination

    sparkConfig.streamingContext.start()


  /*
   Do analysis under her for data from database
   */

    val output= dataFromDatabase.toDF()
    output.createOrReplaceTempView("WordCount")
    val outputSql=sparkConfig.sqlContext.sql("select * from WordCount")
    outputSql.write.mode("append").json("/Users/Olivier/Desktop/Hamubere/SparkExample/src/main/resources/data/countWords/")
    outputSql.show()
    sparkConfig.streamingContext.awaitTermination()

    println("---Spark Stream app closing----")
  }


}
