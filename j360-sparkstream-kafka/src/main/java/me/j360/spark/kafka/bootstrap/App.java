package me.j360.spark.kafka.bootstrap;

import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;
import me.j360.spark.kafka.bootstrap.domain.PlayCount;
import me.j360.spark.kafka.bootstrap.domain.PlayTuple;
import me.j360.spark.kafka.bootstrap.manager.MQClient;
import me.j360.spark.kafka.bootstrap.manager.RedisClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import redis.clients.jedis.Jedis;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters$;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Package: me.j360.spark.kafka.bootstrap
 * User: min_xu
 * Date: 2017/8/7 下午2:40
 * 说明：
 */

@Slf4j
public class App {


    /**
     * 动态获取kafka地址
     * 支持在Master+Worker模式下的分发,连接池基于单例调用
     *
     * @param args
     */
    public static void main(String[] args)  {

        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", Constant.brokerlist);
        kafkaParams.put("group.id", Constant.groupId);

        Set<String> topics = Sets.newHashSet(Constant.topic);


        //Spark
        Duration duration = new Duration(Constant.duration);
        SparkConf sparkConf = new SparkConf().setAppName(Constant.appName);
        if (StringUtils.isNotEmpty(Constant.SPARK_MASTER)) {
            sparkConf.setMaster(Constant.SPARK_MASTER);
        }
        JavaStreamingContext jsctx = new JavaStreamingContext(sparkConf, duration);


        //KafkaCluster
        scala.collection.immutable.Map<String, String> immutableKafkaParam = convert(kafkaParams);
        KafkaCluster kafkaCluster = new KafkaCluster(immutableKafkaParam);

        scala.collection.mutable.Set<String> mutableTopics = JavaConversions
                .asScalaSet(topics);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics
                .toSet();
        scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet = kafkaCluster
                .getPartitions(immutableTopics).right().get();


        //get Offset
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<kafka.common.TopicAndPartition, Long>();
        // 首次消费，默认设置为0
        if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"),
                scalaTopicAndPartitionSet).isLeft()) {
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions
                    .setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                fromOffsets.put(topicAndPartition, 0L);
            }
        } else {
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
                    .getConsumerOffsets(kafkaParams.get("group.id"),
                            scalaTopicAndPartitionSet).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions
                    .mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions
                    .setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                fromOffsets.put(topicAndPartition, offset);
            }
        }


        //
        //Broadcast<HashMap<String, String>> kafkaParamBroadcast = jsctx.sparkContext().broadcast(kafkaParams);

        //JavaInputDStream
        JavaInputDStream<String> stream = KafkaUtils.createDirectStream(jsctx,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                fromOffsets,
                v1 -> v1.message());

        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

        //转化成数据路
        JavaDStream<String> javaDStream = stream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            }
        });

        javaDStream.map(line -> {
            if (StringUtils.startsWith(line, "message: ")) {
                line = StringUtils.substringAfter(line, "message: ");
            }
            String json = StringUtils.trim(line);
            log.info("dmp:= [{}]",json);
            JsonObject resultObject = new JsonParser().parse(json).getAsJsonObject();
            Long vpId = resultObject.get("targetId").getAsLong();
            int count = resultObject.get("count").getAsInt();
            return new PlayCount(vpId, count);
        })
                .mapToPair(new PlayTuple())
                .reduceByKey((i1, i2) -> i1 + i2)
                .foreachRDD(rdd -> {
                    rdd.foreachPartition( partRecords -> {

                        //定义redis对象
                        final RedisClient redisClient = RedisClient.getInstance(Constant.redisIpAddr);

                        //定义MQ对象
                        final MQClient mqClient = MQClient.getInstance(Constant.mqBrokerURL);
                        Session session = mqClient.getSession();
                        MessageProducer messageProducer = mqClient.getMessageProducer();

                        partRecords.forEachRemaining( pair -> {
                            Long vpId = pair._1();
                            Integer count = pair._2();

                            Jedis jedis = redisClient.getResource();
                            //写入到redis
                            String key = String.format(Constant.DMP_VIEW_COUNT, vpId);
                            long viewCount = jedis.incrBy(key, count);
                            if (viewCount > Constant.viewCountNoticeLimit) {
                                String noticeKey = String.format(Constant.DMP_VIEW_NOTICE, vpId);
                                if (!jedis.exists(noticeKey)) {
                                    //发送播放提醒通知
                                    MapMessage message = null;
                                    try {
                                        message = session.createMapMessage();
                                        message.setLong("targetId", vpId);
                                        messageProducer.send(message);
                                        jedis.set(noticeKey, "1");

                                        log.info("发送mq的播放消息:{}",vpId);
                                    } catch (JMSException e) {
                                        log.error("JMS播放消息发送失败 : [vpId={}]", vpId, e);
                                    }
                                }
                            }

                            //释放服务
                            redisClient.returnResource(jedis);
                        });
                    });
                });

        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> v1) throws Exception {
                //OffsetRange[] offsets = ((HasOffsetRanges) arg0.rdd()).offsetRanges();
                //for(OffsetRange o: offsets){
                if (v1.isEmpty()) return;
                List<String> list = v1.collect();
                for(String s:list){
                    log.info("data:[{}]",s);
                }

                for (OffsetRange o : offsetRanges.get()) {
                    // 封装topic.partition 与 offset对应关系 java Map
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

                    // 转换java map to scala immutable.map
                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                            convert(topicAndPartitionObjectMap);

                    // 更新offset到kafkaCluster
                    kafkaCluster.setConsumerOffsets(Constant.groupId, scalatopicAndPartitionObjectMap);
                }
            }
        });

        jsctx.start();
        try {
            jsctx.awaitTermination();
        } catch (InterruptedException e) {
            log.error("结束jsctx异常:",e);
        }
    }


    /**
     * Getting an immutable Scala map is a little tricky because the conversions provided by the collections library return all return mutable ones, and you can't just use toMap because it needs an implicit argument that the Java compiler of course won't provide. A complete solution with that implicit argument looks like this
     * @param m
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> scala.collection.immutable.Map<K, V> convert(Map<K, V> m) {
        return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(
                scala.Predef$.MODULE$.<scala.Tuple2<K, V>>$conforms()
        );
    }

}
