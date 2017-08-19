package me.j360.spark.spring.bootstrap;

import me.j360.spark.spring.configuration.AppConfig;
import me.j360.spark.spring.manager.KafkaManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;


@ComponentScan({"me.j360.spark"})
@EnableAutoConfiguration
@Configuration
public class BootstrapConfiguration {

    @Autowired
    private AppConfig appConfig;

    @Bean
    public KafkaManager kafkaManager() {

        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", appConfig.getGroupId());
        kafkaParams.put("group.id", appConfig.getGroupId());

        return new KafkaManager(kafkaParams);
    }
}
