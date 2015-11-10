package com.asto.dmp.ycd.mq

import com.asto.dmp.ycd.util.{Props, Utils}
import com.rabbitmq.client.{Connection, ConnectionFactory, MessageProperties, Channel}
import org.apache.spark.Logging
import scala.util.parsing.json.JSONObject

object MQAgent extends Logging {
  var channel: Channel = _
  private val connectionFactory = new ConnectionFactory
  private var connection: Connection = _

  connectionFactory.setHost(Props.get("rabbit_mq_host"))
  connectionFactory.setPort(Props.get("rabbit_mq_port").toInt)
  connectionFactory.setUsername(Props.get("rabbit_mq_username"))
  connectionFactory.setPassword(Props.get("rabbit_mq_password"))
  connection = connectionFactory.newConnection
  channel = connection.createChannel

  def send(queueName: String, message: String) {
    logInfo(Utils.logWrapper(s"channel=$channel"))
    //以下四行代码的意思我表示不是很清楚，照搬过来的
    channel.queueDeclare(queueName, true, false, false, null)
    channel.exchangeDeclare(queueName, "direct", true)
    channel.queueBind(queueName, queueName, queueName)
    channel.basicPublish(queueName, queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"))
  }

  def send(queueName: String, messageMap: Map[String, Any]) {
    send(queueName: String, new JSONObject(messageMap).toString())
  }

  def close {

    if (Option(channel).isDefined) channel.close
    if (Option(connection).isDefined) connection.close()
  }

}
