package utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class MySQLDatabaseFunction {

  def readFromMySqlDatabase(sqlContext:SQLContext, url:String,driver:String,username:String,password:String): Unit ={
    val dataframe_mysql=sqlContext.read.format("jdbc").
      option("url", url).option("driver", driver).option("dbtable", "user").
      option("user", username).option("password", password)
      .load()
    dataframe_mysql.show()

  }

  def readFromDatabase(sqlContext:SQLContext,url:String,tableName:String, properties:Properties):DataFrame={
    val dataframe_mysql=sqlContext.read.jdbc(url,tableName, properties).toDF
    return dataframe_mysql
  }

  def writeToDatabase(sqlContext:SQLContext,url:String,tableName:String,dataFrame: DataFrame,
                      properties:Properties) {

    dataFrame.write.mode("append").jdbc(url,tableName, properties)
  }

  def setConnection():Properties={
    val properties=new Properties()
    val urlValue="jdbc:mysql://localhost:3306/spark?allowPublicKeyRetrieval=true&useSSL=false"
    properties.put("user","root")
    properties.put("password","Abcdef@1234")
    return properties
  }

}
