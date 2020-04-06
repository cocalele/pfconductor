package com.netbric.s5.conductor;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.netbric.s5.orm.S5Database;
import org.apache.commons.cli.CommandLine;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import com.netbric.s5.cluster.ClusterManager;

public class CliMain
{
	static final Logger logger = LoggerFactory.getLogger(CliMain.class);
	private static void printUsage()
	{
		System.out.println("Usage: java com.netbric.s5.cli -c <s5_config_file>");
	}
	public static void main(String[] args)
	{
		Options options = new Options();

		// add t option
		options.addOption("c", true, "s5 config file path");
		options.addOption("h", "help", false, "conductor node index");
		
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		try
		{
			cmd = cp.parse(options, args);
		}
		catch (ParseException e1)
		{

			e1.printStackTrace();
			System.exit(1);
			return;
		}
		if(cmd.hasOption("h"))
		{
			printUsage();
			System.exit(1);
		}

		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");

		// zookeeper config file
		String cfgPath = cmd.getOptionValue("c");
		if(cfgPath == null)
		{
			cfgPath = "/etc/s5/s5.conf";
			logger.warn("-c not specified, use {}", cfgPath);
		}
		Config cfg = new Config(cfgPath);

		try
		{
			String zkIp = cfg.getString("zookeeper", "ip", null, true);
			if(zkIp == null)
			{
				System.err.println("zookeeper ip not specified in config file");
				System.exit(1);
			}
			ZooKeeper zk = new ZooKeeper(zkIp, 50000, null);
			List<String> list = zk.getChildren("/s5/conductors", false);
            if(list.size() == 0){
                logger.error("No active conductor found on zk:{}", zkIp);
                System.exit(1);
            }
			String[] nodes = list.toArray(new String[list.size()]);
			Arrays.sort(nodes);
			String leader = new String(zk.getData("/s5/conductors/" + nodes[0], true, null));
            logger.info("Get active conductor:{}", leader);

            logger.info("HTTP started on port 49180");
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			logger.error("Failed to start jconductor:", e1);
		}

    }

}
