package bootstrap;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

/**
 * Package: bootstrap
 * User: min_xu
 * Date: 2017/8/21 下午2:37
 * 说明：
 */

@Slf4j
public class App {

    public static void main(String[] args) {

        Properties properties = new Properties();
        try {
            properties.load(App.class.getResourceAsStream("/spark.properties"));
        } catch (IOException e) {
            //log.error();
        }



    }
}
