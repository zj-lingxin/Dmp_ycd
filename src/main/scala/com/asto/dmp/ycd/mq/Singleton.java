package com.asto.dmp.ycd.mq;

enum Singleton {
	INSTANCE;

	private final Conf conf;

	Singleton() {
		conf = new Conf();
	}

	public static Singleton getInstance() {
		return INSTANCE;
	}

	public Conf getConf() {
		return conf;
	}
	
}