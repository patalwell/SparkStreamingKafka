#!/bin/sh

CHANNEL=cryptocurrency-nifi-data
/usr/local/Cellar/kafka/1.0.0/bin/kafka-topics --delete --zookeeper pathdp1.field.hortonworks.com:2181 --topic $CHANNEL
