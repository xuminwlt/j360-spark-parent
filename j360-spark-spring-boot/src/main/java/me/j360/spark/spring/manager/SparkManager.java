package me.j360.spark.spring.manager;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import me.j360.spark.spring.configuration.AppConfig;
import me.j360.spark.spring.domain.PlayCount;
import me.j360.spark.spring.domain.PlayTuple;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * Package: me.j360.spark.spring.manager
 * User: min_xu
 * Date: 2017/8/19 下午3:04
 * 说明：
 */

@Slf4j
@Component
public class SparkManager {

    @Autowired
    private AppConfig appConfig;
    @Autowired
    private KafkaManager kafkaManager;

    public void start() {
        SparkConf sparkConf = new SparkConf().setAppName(appConfig.getAppName());
        JavaStreamingContext jsctx = new JavaStreamingContext(sparkConf, new Duration(appConfig.getDuration()));

        JavaInputDStream<String> messages = kafkaManager.createDirectStream(jsctx, Collections.singleton(appConfig.getTopic()));
        process(messages);

        kafkaManager.updateZKOffsets(messages);

        jsctx.start();
        try {
            jsctx.awaitTermination();
        } catch (InterruptedException e) {
            log.error("执行异常:{}",appConfig.getAppName(),e);
        }
    }


    public void process(JavaInputDStream<String> messages) {
        log.info("处理试试消息并写入到Redis");
        messages.map(line -> {
            JsonObject resultObject = new JsonParser().parse(line).getAsJsonObject();
            Long vpId = resultObject.get("targetId").getAsLong();
            int count = resultObject.get("count").getAsInt();
            return new PlayCount(vpId, count);
        })
                .mapToPair(new PlayTuple())
                .reduceByKey((i1, i2) -> i1 + i2)
                .foreachRDD(rdd -> {
                    rdd.foreachPartition( partRecords -> {
                        partRecords.forEachRemaining( pair -> {
                            Long vpId = pair._1();
                            Integer count = pair._2();


                        });
                    });
                });
    }

}
