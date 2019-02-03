package main.scala.fr.lenglet.sparktoolbox.write.kafka

import org.apache.kafka.clients.producer._
import java.util.Properties

//https://gist.github.com/fancellu/f78e11b1808db2727d76
//https://www.programcreek.com/scala/index.php?api=org.apache.kafka.clients.producer.ProducerRecord


trait KafkaWriteInterface {
  def setTopic(topics: String): Set[String] {

  }

  def setKafkaParams(brokers: String, zookep: String): Map[String, String] {

  }

  def writeMessages(mess: String, key : String, bootstrap: String, keydeserializer: String, valuedeserializer: String, top: String): Unit {


  }
}

class KafkaWrite extends KafkaWriteInterface {
  override def setTopic(topics: String): Set[String] = {
    val topicsSet = topics.split(",").toSet
    topicsSet
  }

  override def setKafkaParams(brokers: String, zookep: String): Map[String, String] = {
    val MapKafKaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "zookeeper.connect" -> zookep,
      "group.id" -> KafkaWriteConfiguration.groupid,
      "zookeeper.connection.timeout.ms" -> KafkaWriteConfiguration.zookeeperconnectiontimeoutms,
      "security.protocol" -> KafkaWriteConfiguration.securityprotocol,
      "sasl.kerberos.service.name" -> KafkaWriteConfiguration.saslkerberosservicename,
      "auto.offset.reset" -> KafkaWriteConfiguration.autooffsetreset,
      "key.deserializer" -> KafkaWriteConfiguration.keydeserializer,
      "value.deserializer" -> KafkaWriteConfiguration.valuedeserializer)
    MapKafKaParams
  }

  def writeMessages(mess: String, key : String, bootstrap: String, keydeserializer: String, valuedeserializer: String, top: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", keydeserializer)
    props.put("value.serializer", valuedeserializer)
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord(top, key, mess)
    producer.send(record)
    producer.close()
  }
}
