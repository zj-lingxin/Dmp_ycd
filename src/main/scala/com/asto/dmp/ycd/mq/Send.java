package com.asto.dmp.ycd.mq;

public interface Send {

	public void send(String queueName, String message);

}
