package utils

class HbaseFunctions {
//  def setUpHBase(): Unit ={
//    val hbaseConf=HBaseConfiguration.create()
//    hbaseConf.set("hbase.zookeeper.quorum","localhost:2181")
//
//    return hbaseConf
//  }
//  def addEmployeeToHBase(employee:Person, personType:String) {
//    val hbaseConf=HBaseConfiguration.create()
//     hbaseConf.set("hbase.zookeeper.quorum","localhost:2181")
//    val tableName = "employee"
//    val row = Bytes.toBytes(employee.id)
//    val hTable = new HTable(hbaseConf,tableName)
//    val put = new Put(row)
//    val id=Bytes.toBytes("id")
//    val age = Bytes.toBytes("age")
//    val lastName = Bytes.toBytes("lastName")
//    val firstName = Bytes.toBytes("firstName")
//    val address = Bytes.toBytes("address")
//    put.addColumn(Bytes.toBytes(personType), id, Bytes.toBytes(1))
//    put.addColumn(Bytes.toBytes(personType), lastName, Bytes.toBytes("Olivier Ishimwe"))
//    put.addColumn(Bytes.toBytes(personType), firstName, Bytes.toBytes("SE"))
//    put.addColumn(Bytes.toBytes(personType), age, Bytes.toBytes(20))
//    put.addColumn(Bytes.toBytes(personType), address, Bytes.toBytes("Kigali Rwanda"))
//    hTable.put(put)
//    hTable.close()
//  }
//
//  def addCountsToHBase(i:Int,output:String) {
//    val hbaseConf=setUpHBase()
//    val tableName = "word_counts"
//    val hTable = new HTable(hbaseConf,tableName)
//    val put = new Put(Bytes.toBytes(i))
//
////    put.addColumn(Bytes.toBytes("words"), wordsList., Bytes.toBytes(1))
////    put.addColumn(Bytes.toBytes(personType), lastName, Bytes.toBytes("Olivier Ishimwe"))
////    put.addColumn(Bytes.toBytes(personType), firstName, Bytes.toBytes("SE"))
////    put.addColumn(Bytes.toBytes(personType), age, Bytes.toBytes(20))
////    put.addColumn(Bytes.toBytes(personType), address, Bytes.toBytes("Kigali Rwanda"))
//    hTable.put(put)
//    hTable.close()
//  }

}
