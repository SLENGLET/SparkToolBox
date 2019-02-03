package fr.lenglet.sparktoolbox.exercices

import main.scala.fr.lenglet.sparktoolbox.read.kafka.KafkaRead
import main.scala.fr.lenglet.sparktoolbox.write.kafka.{KafkaWrite, KafkaWriteConfiguration}
import main.scala.fr.lenglet.sparktoolbox.write.orientdb.OrientDBWrite
import main.scala.fr.lenglet.sparktoolbox.write.orientdb.OrientDBWriteConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies}
import org.apache.tinkerpop.gremlin.orientdb.{OrientGraph, OrientGraphFactory}

/**
  * Date :::: 19/01/2019
  * Exercice :::: read messages from kafka topic , write in orientdb dbgraph and kafka topic output
  *
  */

object Exercice5 {
  def main(args: Array[String]) {

    val Array(brokers, zookep, topics) = args
    val sparkConf = new SparkConf().setAppName("Exercice5")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kread = new KafkaRead()
    val owrite = new OrientDBWrite()
    val kwrite = new KafkaWrite


    /* Kerberos */

    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
    System.setProperty("sun.security.krb5.debug", "true")

    val messages = kread.createMessages(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](kread.setTopic(topics), kread.setKafkaParams(brokers, zookep)))

    messages.foreachRDD(rdd =>
      if (!rdd.partitions.isEmpty)

        rdd.foreachPartition(partition => {
          val uri: String = "remote:" + OrientDBWriteConfiguration.host + ":" + OrientDBWriteConfiguration.port + "/database/" + OrientDBWriteConfiguration.base + ""
          val factory: OrientGraphFactory = new OrientGraphFactory(uri)
          val graph: OrientGraph = factory.getTx()

          try {

            partition.foreach(p => {

              println("### element ### " + p.value())
              owrite.saveVertex(graph,p.value(),OrientDBWriteConfiguration.oclass)
              kwrite.writeMessages(p.value()+" écrit dans orient",p.key(),brokers,KafkaWriteConfiguration.keydeserializer,KafkaWriteConfiguration.valuedeserializer,KafkaWriteConfiguration.topicoutput)

            })
            graph.commit()

          }
          catch {

            case unformat => println("### unformat exception ###" + unformat)
              graph.rollback()

          }
          finally {
            println("### Fin de cycle, on close")
            graph.close()
          }
        })
    )

    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()
  }
}

