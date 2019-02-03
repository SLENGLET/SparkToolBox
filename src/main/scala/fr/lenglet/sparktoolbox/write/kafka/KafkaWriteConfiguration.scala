package main.scala.fr.lenglet.sparktoolbox.write.kafka

object KafkaWriteConfiguration {

  val groupid ="default"
  val zookeeperconnectiontimeoutms = "1000"
  val securityprotocol ="SASL_PLAINTEXT"
  val saslkerberosservicename = "kafka"
  val autooffsetreset = "latest"
  val keyserializer = "org.apache.kafka.common.serialization.StringSerializer"
  val valueserializer="org.apache.kafka.common.serialization.StringSerializer"
  val topicoutput ="topic.output"
  val producertype="async"
  val requestrequiredacks="1"


}
