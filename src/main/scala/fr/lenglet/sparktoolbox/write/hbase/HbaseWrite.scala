package main.scala.fr.lenglet.sparktoolbox.write.hbase

import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes

trait HbaseWriteInterface {
  def save(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Unit = {
  }
}

class HbaseWrite extends HbaseWriteInterface {
  override def save(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Unit = {
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
    table.put(put)
  }
}
