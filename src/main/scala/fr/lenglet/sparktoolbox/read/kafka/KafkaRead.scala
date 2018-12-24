package main.scala.fr.lenglet.sparktoolbox.read.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream

import org.apache.spark.streaming.kafka010._

trait KafkaReadInterface {

  def setTopic(topics: String): Set[String] {

  }

  def setKafkaParams(brokers: String, zookep: String): Map[String, String] {

  }

  def createMessages(ssc: StreamingContext, ls: LocationStrategy, cs: ConsumerStrategy[String, String]): InputDStream[ConsumerRecord[String, String]] {

  }
}

class KafkaRead extends KafkaReadInterface {

  override def setTopic(topics: String): Set[String] = {
    val topicsSet = topics.split(",").toSet
    topicsSet
  }

  override def setKafkaParams(brokers: String, zookep: String): Map[String, String] = {
    val MapKafKaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "zookeeper.connect" -> zookep,
      "group.id" -> KafkaReadConfiguration.groupid,
      "zookeeper.connection.timeout.ms" -> KafkaReadConfiguration.zookeeperconnectiontimeoutms,
      "security.protocol" -> KafkaReadConfiguration.securityprotocol,
      "sasl.kerberos.service.name" -> KafkaReadConfiguration.saslkerberosservicename,
      "auto.offset.reset" -> KafkaReadConfiguration.autooffsetreset,
      "key.deserializer" -> KafkaReadConfiguration.keydeserializer,
      "value.deserializer" -> KafkaReadConfiguration.valuedeserializer)
    MapKafKaParams
  }

  override def createMessages(ssc: StreamingContext, ls: LocationStrategy, cs: ConsumerStrategy[String, String]): InputDStream[ConsumerRecord[String, String]] = {
    val messages = KafkaUtils.createDirectStream[String, String](ssc,ls,cs)
    messages
  }

}


