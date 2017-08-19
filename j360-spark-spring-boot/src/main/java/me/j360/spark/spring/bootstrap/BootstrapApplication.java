package me.j360.spark.spring.bootstrap;

import org.springframework.boot.SpringApplication;

/**
 * Package: me.j360.disboot.bootstrap
 * User: min_xu
 * Date: 2017/6/1 下午6:02
 * 说明：
 */


public class BootstrapApplication {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication springApplication = new SpringApplication(BootstrapConfiguration.class);
        springApplication.addListeners(new ApplicationStartup());
        springApplication.run(args);

    }

}
