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
	// static String ssh_bin = "c:/eclipse/plink.exe";
	static String ssh_bin = "/usr/bin/plink";  // Liunx need to install plink
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
		// cl.addArgument("123456"); // plink
		cl.addArgument("Flysl1ce");  //122:flyslice
		cl.addArgument("-l");
		cl.addArgument("root");

		// cl.addArgument("-p");
		// cl.addArgument("822");
		cl.addArgument(targetIp);
		// cl.addArgument("source /etc/profile; " + cmdLine);
		String remoteCmd = "source /etc/profile; " + cmdLine;
		remoteCmd = remoteCmd.replace("\"", "\\\"");
		cl.addArgument(remoteCmd, false);
		logger.info("cmd after sshexec: {}", remoteCmd);

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
			// 修复2：清除输出中的换行符、回车符，并去除首尾空白
			String output = stdouterr.toString("UTF-8");
			// 替换所有换行和回车符
			output = output.replaceAll("[\n\r\t]", " ");
			// 去除首尾空白
			output = output.trim();
			// 合并多个空格为一个
			output = output.replaceAll("\\s+", " ");
			return output;
			// return stdouterr.toString("UTF-8");
		}
		catch (UnsupportedEncodingException e)
		{

			e.printStackTrace();
		}
		return null;
	}
}
