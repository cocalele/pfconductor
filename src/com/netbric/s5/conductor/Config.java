package com.netbric.s5.conductor;

import java.io.File;
import java.io.IOException;

import org.ini4j.Wini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config
{
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

	public String getString(String section, String key, String defaultVal)
	{
		return cfg.get(section, key, String.class);
	}
}
