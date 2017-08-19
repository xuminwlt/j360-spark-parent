package me.j360.spark.spring.bootstrap;

import me.j360.spark.spring.manager.SparkManager;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Package: me.j360.spark.spring.bootstrap
 * User: min_xu
 * Date: 2017/8/19 下午3:09
 * 说明：
 */
public class ApplicationStartup implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        SparkManager sparkManager = event.getApplicationContext().getBean(SparkManager.class);
        sparkManager.start();
    }
}
