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