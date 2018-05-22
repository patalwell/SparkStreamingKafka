#!/bin/sh

CHANNEL=cryptocurrency-market-data
/usr/local/Cellar/kafka/1.0.0/bin/kafka-console-consumer --zookeeper pathdf3.field.hortonworks.com:2181 --topic $CHANNEL --from-beginning
