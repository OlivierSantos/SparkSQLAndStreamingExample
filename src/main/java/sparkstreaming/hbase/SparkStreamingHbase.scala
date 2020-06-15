package sparkstreaming.hbase

import java.text.SimpleDateFormat
import java.util.Date

import model.Property
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkStreamingHbase {

  def main(args: Array[String]): Unit ={
    println("--- Spark Streaming App Starting ----")
    val brokers="localhost:9092"
    val groupId="GRP1"
    val topics="retail,stream,news"

    println("Olivier is starting a new scala app")
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("StreamingApp")

    val streamingContext= new org.apache.spark.streaming.StreamingContext(sparkConf,Seconds(3))
    val sparkContext= streamingContext.sparkContext
    sparkContext.setLogLevel("OFF")
    val sqlContext=new org.apache.spark.sql.SQLContext(sparkContext)
    val topicSet=topics.split((",")).toSet
    val kafKaparms=Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    import sqlContext.implicits._
    //val hbaseConf = HBaseConfiguration.create()
    val tablename = "employee"

    val hbaseConf = HBaseConfiguration.create()
    //hbaseConf.clear()

    hbaseConf.set("hbase.zookeeper.quorum", "localhost");
    //hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set(TableInputFormat.INPUT_TABLE,tablename)
    println(hbaseConf.getClassLoader)

    val dataFromHBase= sparkContext.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],classOf[Result])
    val employeeFromHBase=dataFromHBase.mapPartitions(f=>f.map(row1=>(Bytes.toString(row1._2.getRow),
      Bytes.toString(row1._2.getValue(Bytes.toBytes("profile"),Bytes.toBytes("name"))),
      Bytes.toString(row1._2.getValue(Bytes.toBytes("profile"),Bytes.toBytes("address"))))))
      .toDF("id","name","address")

    employeeFromHBase.createOrReplaceTempView("Employee")

    val employee= sqlContext.sql("select * from Employee")
    employee.show()




    val messages1= KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafKaparms));

    val lines =messages1.map(_.value)
    //line.print()
    val words= lines.flatMap(_.split(","))
   // words.print()
    val wordCounts= words.map(x=>(x, 1L)).reduceByKey(_+_)
    print("Checking the value:")
    //wordCounts.print()

    lines.foreachRDD( rdd => {
      if (rdd.count() > 0) {
        val df = rdd.toDF


        val simpleDateFormat= new SimpleDateFormat("")
        val date= new Date(System.currentTimeMillis())
        simpleDateFormat.format(date)
        val prop = new Property(1,lines.toString,date)
        println("Testing:"+prop.id+","+prop.content+prop.dataCreate)

        //df.write.jdbc(s"jdbc:mysql://localhost:3306/spark", "streaming",prop)

       // df.write.format("parquet").mode("append").save("/Users/Olivier/Desktop/SparkStreamingExample/src/main/resources/data/output")
        //df.write.csv("/Users/Olivier/Desktop/SparkStreamingExample/src/main/resources/data/output.csv")
      }
    }
    )
    streamingContext.start()
    streamingContext.awaitTermination()



    println("---Spark Stream app closing----")
  }

}
