## Spark生成环境演练

### Spark批处理


### Spark Streaming实时数据分析

1. 启动kafka

./bin/kafka-server-start.sh config/server.properties

2. 生成kafka dmp消息

j360-kafka demo

3. 执行实时任务

spark-submit --master local target/j360-sparkstream-kafka-1.0-SNAPSHOT.jar


4. 检查redis数据是否生成

$redis-cli
$get key


### Flume环境

flume-ng agent -n dmpagent -c conf -f conf/flume-conf.properties.template

flume.properties

```
agent.sources = s1
agent.channels = c1
agent.sinks = k1

# For each one of the sources, the type is defined
agent.sources.s1.type = exec
agent.sources.s1.shell = /bin/bash -c
agent.sources.s1.command = tail -F /usr/local/tv-log/dmp/dmp.log
agent.sources.s1.channels = c1
# The channel can be defined as follows.

# Each sink's type must be defined
agent.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
agent.sinks.k1.kafka.bootstrap.servers=10.253.15.182:9092
agent.sinks.k1.kafka.topic=dmp
agent.sinks.k1.partition.key=0
agent.sinks.k1.partitioner.class=org.apache.flume.plugins.SinglePartition
agent.sinks.k1.serializer.class=kafka.serializer.StringEncoder
agent.sinks.k1.request.required.acks=0
agent.sinks.k1.max.message.size=1000000
agent.sinks.k1.producer.type=sync
#Specify the channel the sink should use
agent.sinks.k1.channel = c1
```