package fr.lenglet.sparktoolbox.exercices

import kafka.serializer.StringDecoder

/**
  * Date :::: 10/08/2018
  * Exercice :::: read messages from kafka topic , write on hbase table
  *
  */

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
//import _root_.kafka.serializer.DefaultDecoder
//import _root_.kafka.serializer.StringDecoder
//import org.apache.spark.storage.StorageLevel
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.Duration;

import org.apache.hadoop.hbase.{TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration
import java.security.PrivilegedAction
import org.apache.hadoop.hbase.client.{HTableInterface, HConnectionManager, HConnection, HBaseAdmin}
import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes

object Exercice4 {
  def main(args: Array[String]) {


    val Array(brokers, zookep, topics) = args


    val sparkConf = new SparkConf().setAppName("Exercice4")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "zookeeper.connect" -> zookep,
      "group.id" -> "default",
      "zookeeper.connection.timeout.ms" -> "1000",
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.kerberos.service.name" -> "kafka",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    /* Kerberos */

    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    messages.foreachRDD(rdd =>
      if (!rdd.partitions.isEmpty)

        rdd.foreachPartition(partition => {

          val table_name = "lenglet_exercice:Personne"
          val hbase_conf = HBaseConfiguration.create()
          hbase_conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
          hbase_conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
          hbase_conf.set("hbase.rpc.controllerfactory.class", "org.apache.hadoop.hbase.ipc.RpcControllerFactory")
          hbase_conf.set("hbase.zookeeper.quorum", "datanode1.lenglet.fr,datanode2.lenglet.fr,datanode3.lenglet.fr")
          hbase_conf.set("hbase.zookeeper.property.clientPort", "2181")
          hbase_conf.set("hbase.master", "datanode2.lenglet.fr:16000")
          hbase_conf.set("zookeeper.znode.parent", "/hbase-secure")
          hbase_conf.set("hadoop.security.authentication", "kerberos")

          UserGroupInformation.setConfiguration(hbase_conf)
          if (UserGroupInformation.isSecurityEnabled) {

            val loggedUGI: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("dco_app_edma@LENGLET.FR", "/etc/security/keytabs/dco_app_edma.keytab")
            val c: Configuration = hbase_conf
            loggedUGI.doAs(new PrivilegedAction[Void] {
              override def run() = {
                try {

                  val hConnection: HConnection = HConnectionManager.createConnection(c)
                  val table: HTableInterface = hConnection.getTable(table_name)


                  partition.foreach(p => {
                    val rowkey = String.valueOf(System.currentTimeMillis() / 1000)
                    println("topic : " + p.topic() + " message : " + p.value())
                    PutToHbase(rowkey, "messages", p.value(), "d", table)
                  })

                }
                null
              }
            })
          }
        })
    )

    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()
  }

  def PutToHbase(rowkey: String, qualifier: String, message: String, cf: String, table: HTableInterface): Unit = {

    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
    table.put(put)

  }
}
