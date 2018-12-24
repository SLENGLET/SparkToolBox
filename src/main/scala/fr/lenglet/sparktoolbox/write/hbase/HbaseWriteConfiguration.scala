package main.scala.fr.lenglet.sparktoolbox.write.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration

object HbaseWriteConfiguration {

  val table_name = "lenglet_exercice:Personne"
  val h = HBaseConfiguration.create()
  h.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  h.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
  h.set("hbase.rpc.controllerfactory.class", "org.apache.hadoop.hbase.ipc.RpcControllerFactory")
  h.set("hbase.zookeeper.quorum", "datanode1.lenglet.fr,datanode2.lenglet.fr,datanode3.lenglet.fr")
  h.set("hbase.zookeeper.property.clientPort", "2181")
  h.set("hbase.master", "datanode2.lenglet.fr:16000")
  h.set("zookeeper.znode.parent", "/hbase-secure")
  h.set("hadoop.security.authentication", "kerberos")
  val hbase_conf = h
}
