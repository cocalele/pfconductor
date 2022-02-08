package com.netbric.s5.conductor.exception;

public class LoggedException extends StateException {
	public LoggedException(org.slf4j.Logger log, String format, Object... args)  {
		super(String.format(format, args));
		log.error(getMessage());
	}
}
