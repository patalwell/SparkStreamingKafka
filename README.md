Spark Streaming and Structured Streaming with PySpark and Kafka

Background: I was trying to familiarize myself with Spark's streaming libraries; but wound up running into dependency issues as it relates to Kafka 10 and Pyspark Structured Streaming, which is still experimental. The spark streaming application contains logic that grabs a kafka topic and applies DF transfomrations in order to create a data frame, cast a schema, and derive statistical values in realtime.
