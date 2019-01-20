package main.scala.fr.lenglet.sparktoolbox.write.hbase

import org.apache.hadoop.hbase.client.{HTableInterface, Put, Row}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._

trait HbaseWriteInterface extends Serializable {

  def setTableParams(table: String): String {
  }

  def save(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Unit {
  }

  def saveBatch(lr: List[Row], table: HTableInterface, rs: Array[AnyRef]): Unit {
  }

  def createList(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Put {
  }
}


class HbaseWrite extends HbaseWriteInterface {

  override def setTableParams(table: String): String = {
    table
  }

  override def save(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Unit = {
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
    table.put(put)
  }

  override def saveBatch(lr: List[Row], table: HTableInterface, rs: Array[AnyRef]): Unit = {
    try {
      table.batch(lr.asJava, rs)
    }
    catch {
      case unformat => {
        println("### unformat hbase exception ###" + unformat)
      }
    }
  }

  override def createList(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Put = {
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
    put
  }
}