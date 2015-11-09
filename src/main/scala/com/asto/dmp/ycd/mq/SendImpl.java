package com.asto.dmp.ycd.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SendImpl{
    private static final String DIRECT = "direct";
    static Channel channel = null;
    public static void send(String queueName, String message) {
        String exchangeName = queueName;
        try {
            channel = Singleton.getInstance().getConf().getChannel();
            System.out.println("++++++++++++++channel=" + channel);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.exchangeDeclare(exchangeName, DIRECT, true);
            channel.queueBind(queueName, exchangeName, queueName);
            channel.basicPublish(exchangeName, queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("++++++++++++++mq");
        }
    }
    public static void close(){
        System.out.println("++++++++++++++channel");
        try {
            if (channel != null)
                channel.close();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
