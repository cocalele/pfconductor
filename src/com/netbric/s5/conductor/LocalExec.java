package com.netbric.s5.conductor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExec
{
	static Map<String, String> env = new HashMap<String, String>();

	static
	{
		env.put("LANG", "en_US.UTF-8");
	}

	ByteArrayOutputStream stdouterr = new ByteArrayOutputStream(8192);
	final Logger logger = LoggerFactory.getLogger(LocalExec.class);

	public int execute(String cmdLine, InputStream stdin) throws IOException
	{
		CommandLine cl = new CommandLine(cmdLine);
		DefaultExecutor exec = new DefaultExecutor();
		PumpStreamHandler inout = new PumpStreamHandler(stdouterr, stdouterr, stdin);
		exec.setStreamHandler(inout);
		stdouterr.reset();

		try
		{
			return exec.execute(cl, env);
		}
		catch (ExecuteException e)
		{
			return e.getExitValue();
		}

	}

	public String getStdout()
	{
		try
		{
			return stdouterr.toString("UTF-8");
		}
		catch (UnsupportedEncodingException e)
		{

			e.printStackTrace();
		}
		return null;
	}
}
