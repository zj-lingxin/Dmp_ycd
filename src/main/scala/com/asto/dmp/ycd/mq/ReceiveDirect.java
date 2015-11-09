package com.asto.dmp.ycd.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class ReceiveDirect {
	private static final String QUEUE_NAME = "qz";
	private static final String EXCHANGE_NAME = "qz";
	private static final String DIRECT = "direct";

	public static void main(String[] argv) throws Exception {
		Conf conf=new Conf();
		Channel channel = conf.getChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, DIRECT);
		channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, QUEUE_NAME);
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(QUEUE_NAME, true, consumer);

		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			String message = new String(delivery.getBody(), "UTF-8");
			String routingKey = delivery.getEnvelope().getRoutingKey();
			System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");
		}
	}
}
