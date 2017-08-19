package me.j360.spark.spring.configuration;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Package: me.j360.spark.spring.configuration
 * User: min_xu
 * Date: 2017/8/19 下午2:59
 * 说明：
 */

@Configuration
@Data
public class AppConfig {


    @Value("${app.name:j360-spark-spring-boot}")
    private String appName;

    @Value("${app.topic}")
    private String topic;

    @Value("${app.duration}")
    private Long duration;

    @Value("${app.groupId}")
    private String groupId;

    @Value("${app.brokerlist}")
    private String brokerlist;

}
