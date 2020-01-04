package com.netbric.s5.conductor;

import java.io.File;
import java.io.IOException;

import org.ini4j.Wini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config
{
	public static final long DEFAULT_SHARD_SIZE=64L<<30;
	final Logger logger = LoggerFactory.getLogger(Config.class);
	Wini cfg;

	public Config(String path)
	{
		cfg = new Wini();
		try
		{
			cfg.load(new File(path));
		}
		catch (IOException e)
		{
			logger.error("Fail to load config file:{}", path);
			e.printStackTrace();
			System.exit(1);
		}
	}
	public String getString(String section, String key, String defaultVal, boolean mandatory) throws  ConfigException
	{
		String rst = cfg.get(section, key, String.class);
		if(rst != null)
			return rst;
		if(mandatory)
			throw new ConfigException(String.format("need config item %s.%s", section, key));
        return defaultVal;
	}
	public String getString(String section, String key, String defaultVal)throws  ConfigException
	{
		return getString(section, key, defaultVal, false);
	}
}
