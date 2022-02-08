package com.netbric.s5.conductor.exception;

/**
 * Created by liulele on 2019/11/24.
 */
public class ConfigException extends Exception {
    public ConfigException(String s) {
        super(s);
    }

    public ConfigException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
