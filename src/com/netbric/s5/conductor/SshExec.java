package com.netbric.s5.conductor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SshExec
{
	static String ssh_bin = "c:/eclipse/plink.exe";
	static Map<String, String> env = new HashMap<String, String>();

	static
	{
		env.put("LANG", "en_US.UTF-8");
	}

	public String targetIp;
	ByteArrayOutputStream stdouterr = new ByteArrayOutputStream(8192);
	final Logger logger = LoggerFactory.getLogger(SshExec.class);

	public SshExec(String ip)
	{
		this.targetIp = ip;
	}

	public String lastCli;

	public int execute(String cmdLine) throws IOException
	{
		CommandLine cl = new CommandLine(ssh_bin);
		cl.addArgument("-T");
		// cl.addArgument("-n"); //use ssh
		cl.addArgument("-pw"); // plink
		cl.addArgument("123456"); // plink
		cl.addArgument("-l");
		cl.addArgument("root");

		// cl.addArgument("-p");
		// cl.addArgument("822");
		cl.addArgument(targetIp);
		cl.addArgument("source /etc/profile; " + cmdLine);
		DefaultExecutor exec = new DefaultExecutor();
		exec.setStreamHandler(new PumpStreamHandler(stdouterr));
		stdouterr.reset();
		logger.debug("Execute command on: {},{}", targetIp, cl.toString());
		try
		{
			lastCli = cl.toString();
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
