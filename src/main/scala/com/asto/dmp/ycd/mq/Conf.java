package com.asto.dmp.ycd.mq;

import com.asto.dmp.ycd.util.MQUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Conf {

	public Conf() {
		try {
			Connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static final String HOST = "rabbitmq.host";
	private static final String PORT = "rabbitmq.port";
	private static final String USER_NAME = "rabbitmq.username";
	private static final String PASSWORD = "rabbitmq.password";
	private static Connection connection;
	private static Channel channel;
	private static ConnectionFactory factory;

/*	public Map<String, String> GetAllProperties() throws IOException {
		FileUtil.getPropByKey("rabbit_mq_host")
		FileUtil.getPropByKey("rabbit_mq_port")
		FileUtil.getPropByKey("rabbit_mq_username")
		FileUtil.getPropByKey("rabbit_mq_password")
		Properties pps = new Properties();
		InputStream in = new BufferedInputStream(getClass().getClassLoader().getResourceAsStream("application.properties"));
		pps.load(in);
		@SuppressWarnings("rawtypes")
		Enumeration en = pps.propertyNames();
		Map<String, String> map = new HashMap<String, String>();
		System.out.println("*********************=="+en.hasMoreElements());
		while (en.hasMoreElements()) {
			String strKey = (String) en.nextElement();
			String strValue = pps.getProperty(strKey);
			System.out.println(strKey+":"+strValue);
			map.put(strKey, strValue);
		}
		return map;
	}*/

	public void Connect() throws IOException, TimeoutException {
		//Map<String, String> map = GetAllProperties();
		factory = new ConnectionFactory();
		factory.setHost(MQUtils.getPropByKey("rabbit_mq_host"));
		factory.setPort(Integer.valueOf(MQUtils.getPropByKey("rabbit_mq_port")));
		factory.setUsername(MQUtils.getPropByKey("rabbit_mq_username"));
		factory.setPassword(MQUtils.getPropByKey("rabbit_mq_password"));
		connection = factory.newConnection();
		channel = connection.createChannel();
	}

	public static Channel getChannel() {
		return channel;
	}

	public static Connection getConnection() {
		return connection;
	}

	public static void setConnection(Connection connection) {
		Conf.connection = connection;
	}

	public static void setChannel(Channel channel) {
		Conf.channel = channel;
	}

	public static ConnectionFactory getFactory() {
		return factory;
	}

	public static void setFactory(ConnectionFactory factory) {
		Conf.factory = factory;
	}
}
