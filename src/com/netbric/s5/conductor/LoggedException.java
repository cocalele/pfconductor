package com.netbric.s5.conductor;

public class LoggedException extends Exception {
	public LoggedException(org.slf4j.Logger log, String format, Object... args)  {
		super(String.format(format, args));
		log.error(getMessage());
	}
}
