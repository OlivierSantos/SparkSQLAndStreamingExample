package config

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

class SparkConfig {
  val sparkConf=new SparkConf().setMaster("local[*]").setAppName("StreamingApp")
  val streamingContext= new org.apache.spark.streaming.StreamingContext(sparkConf,Seconds(10))
  val sparkContext= streamingContext.sparkContext
  sparkContext.setLogLevel("OFF")
  val sqlContext=new org.apache.spark.sql.SQLContext(sparkContext)

}
