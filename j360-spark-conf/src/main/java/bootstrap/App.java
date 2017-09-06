package bootstrap;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        // Loading data from a JDBC source
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .load();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "username");
        connectionProperties.put("password", "password");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);




        


    }
}
