#!/bin/bash

export JAVA_HOME=/etc/alternatives/jre_1.7.0/
export SPARK_MAJOR_VERSION=2

spark-submit \
  --class fr.lenglet.sparktoolbox.exercices.$1\
  --name spark.lenglet.job \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 1 --executor-memory 512M --driver-memory 512M --executor-cores 1 \
  --principal app_toolbox@LENGLET.FR --keytab /etc/security/keytabs/app_toolbox.keytab \
  --files "/home/app_toolbox/jaas.conf" \
  --driver-java-options "-Djava.security.auth.login.config=./jaas.conf" \
  --conf "spark.yarn.security.tokens.hive.enabled=false" \
  --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.protocol=PLAINTEXTSASL" \/tmp/sparktoolbox-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
datanode1.lenglet.fr:6667,datanode3.lenglet.fr:6667,datanode2.lenglet.fr:6667 datanode1.lenglet.fr:2181,datanode3.lenglet.fr:2181,datanode2.lenglet.fr:2181 topic.input

