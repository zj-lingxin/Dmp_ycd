package com.asto.dmp.ycd.util.mail

import com.asto.dmp.ycd.util.Props

class Mail(var privateContext: String, var subject: String = Props.get("mail_subject"), var to: String = Props.get("mail_to")) {
  //发件箱
  var from = Props.get("mail_from")
  //发件箱的密码
  var password = Props.get("mail_password")
  //简单邮件传送协议服务器
  var smtpHost = Props.get("mail_smtpHost")
  //抄送给哪些邮箱，多个邮箱之前用“，”分隔
  var cc = Props.get("mail_cc")
  //密送给哪些邮箱，多个邮箱之前用“，”分隔
  var bcc = Props.get("mail_bcc")

  def this(t: Throwable) {
    this("")
    context_=(t)
  }

  def this(t: Throwable, subject: String) {
    this(t)
    this.subject = subject
  }

  def this(t: Throwable, subject: String, to: String) {
    this(t, subject)
    this.to = to
  }

  def context_=(t: Throwable) {
    this.privateContext = t + "\n" + t.getStackTraceString
  }

  def context_=(context: String) {
    this.privateContext = context
  }

  def context: String = {
    this.privateContext
  }

  def setTo(to: String): this.type = {
    this.to = to
    this
  }

  def setFrom(from: String): this.type = {
    this.from = from
    this
  }

  def setPassword(password: String): this.type = {
    this.password = password
    this
  }

  def setSmptHost(smtpHost: String): this.type = {
    this.smtpHost = smtpHost
    this
  }

  def setSubject(subject: String): this.type = {
    this.subject = subject
    this
  }

  def setContext(context: String): this.type = {
    this.privateContext = context
    this
  }

  def setContext(t: Throwable): this.type = {
    context_=(t)
    this
  }

  def setCc(cc: String): this.type = {
    this.cc = cc
    this
  }

  def setBcc(bcc: String): this.type = {
    this.bcc = bcc
    this
  }

  override def toString: String = {
    s"\nto:$to\nsubject:$subject\nfrom:$from\npassword:$password \nsmtpHost:$smtpHost\ncc:$cc\nbcc:$bcc \n"
  }
}

