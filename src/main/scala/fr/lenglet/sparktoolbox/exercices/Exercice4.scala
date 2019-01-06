package fr.lenglet.sparktoolbox.exercices

import java.io.IOException

import kafka.serializer.StringDecoder
import main.scala.fr.lenglet.sparktoolbox.read.kafka.KafkaRead
import main.scala.fr.lenglet.sparktoolbox.write.hbase.HbaseWrite
import main.scala.fr.lenglet.sparktoolbox.write.hbase.HbaseWriteConfiguration

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
import org.apache.hadoop.hbase.client.{HTableInterface, Put, Row}
import org.apache.hadoop.hbase.util.Bytes

object Exercice4 {
  def main(args: Array[String]) {

    val Array(brokers, zookep, topics) = args
    val sparkConf = new SparkConf().setAppName("Exercice4")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kread = new KafkaRead();
    val hwrite = new HbaseWrite();

    /* Kerberos */

    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    val messages = kread.createMessages(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](kread.setTopic(topics), kread.setKafkaParams(brokers,zookep)))

    messages.foreachRDD(rdd =>
      if (!rdd.partitions.isEmpty)

        rdd.foreachPartition(partition => {

          UserGroupInformation.setConfiguration(HbaseWriteConfiguration.hbase_conf)
          if (UserGroupInformation.isSecurityEnabled) {

            val loggedUGI: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("app_toolbox@LENGLET.FR", "/etc/security/keytabs/app_toolbox.keytab")
            val c: Configuration = HbaseWriteConfiguration.hbase_conf
            loggedUGI.doAs(new PrivilegedAction[Void] {
              override def run() = {
                try {

                  val hConnection: HConnection = HConnectionManager.createConnection(c)
                  val table: HTableInterface = hConnection.getTable(HbaseWriteConfiguration.table_name)
                  var lr = List[Row]()


                  partition.foreach(p => {

                    val rowkey = String.valueOf(System.currentTimeMillis() / 1000)
                    println("topic : " + p.topic() + " message : " + p.value())
                    var lp = hwrite.createList(rowkey, "messages", p.value(), "d", table)
                    lr = lp :: lr
                    //hwrite.save(rowkey, "messages", p.value(), "d", table)
                  })

                  val results = new Array[AnyRef](lr.length)
                  hwrite.saveBatch(lr, table, results)
                  println(lr.length+"### ARRAY ###" +results.mkString(","))
                  hConnection.close()

                }
                catch{

                  case ia: IllegalArgumentException => println("illegal arg. exception")
                  case is: IllegalStateException    => println("illegal state exception")
                  case io: IOException              => println("IO exception")
                  case unformat => {
                    println("### unformat exception ###" + unformat)
                  }
                }
                finally {
                  println("### Fin de cycle, on close")
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
}
