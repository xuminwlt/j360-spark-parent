package me.j360.spark.kafka.bootstrap.manager;

import lombok.extern.slf4j.Slf4j;
import me.j360.spark.kafka.bootstrap.Constant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;



@Slf4j
public class MQClient {

    public static Connection connection;
    public static Session session;
    public static ConnectionFactory factory;
    public static MessageProducer messageProducer;

    public String host;

    public MQClient(){
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
    }

    private MQClient(String host){
        this.host = host;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());

        factory = new ActiveMQConnectionFactory(host);
        try {
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageProducer = session.createProducer(new ActiveMQQueue(Constant.mqDestimate));
        } catch (JMSException e) {
            log.error("JMS链接失败: [{}]" , Constant.mqBrokerURL,e);
        }

    }

    private static MQClient singleton;

    public static MQClient getInstance(String host) {
        if (singleton == null) {
            synchronized (RedisClient.class) {
                if (singleton == null) {
                    singleton = new MQClient(host);
                }
            }
        }
        return singleton;
    }

    static class CleanWorkThread extends Thread{
        @Override
        public void run() {
            if (null != connection){
                try {
                    session.close();
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                session = null;
                connection = null;
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public Session getSession() {
        return session;
    }

    public MessageProducer getMessageProducer() {
        return messageProducer;
    }
}