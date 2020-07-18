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

import com.netbric.s5.cluster.ClusterManager;

public class Main
{
	static {
		System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy/MM/dd H:mm:ss.SSS]");

	}
	static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static void printUsage()
	{
		System.out.println("Usage: java com.netbric.s5.conductor -c <s5_config_file>");
	}
	public static void main(String[] args)
	{
		Options options = new Options();

		// add t option
		options.addOption("c", true, "s5 config file path");
		options.addOption("i", true, "conductor node index");
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

		InetAddress ia = null;
		try
		{
			ia = InetAddress.getLocalHost();
		}
		catch (UnknownHostException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String managmentIp = ia.getHostAddress();

		try
		{
			managmentIp = cfg.getString("conductor" , "mngt_ip", managmentIp);
			String zkIp = cfg.getString("zookeeper", "ip", null, true);
			if(zkIp == null)
			{
				System.err.println("zookeeper ip not specified in config file");
				System.exit(1);
			}
			ClusterManager.registerAsConductor(managmentIp, zkIp);
			ClusterManager.waitToBeMaster(managmentIp);
			S5Database.getInstance().init(cfg);
			ClusterManager.zkHelper.createZkNodeIfNotExist("/s5/stores", null);
			ClusterManager.watchStores();
			ClusterManager.updateStoresFromZk();

			// Start the server
			org.eclipse.jetty.server.Server srv = new org.eclipse.jetty.server.Server(49180);
			// Add a single handler on context "/hello"
			ContextHandler context = new ContextHandler();
			context.setContextPath("/s5c");
			context.setHandler(new S5RestfulHandler());
			srv.setHandler(context);
			srv.start();
			logger.info("HTTP started on port 49180");
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			logger.error("Failed to start jconductor:{}", e1);
		}

        // Can be accessed using http://localhost:8080/hello


//        try
//        {
//            HttpServer server = HttpServer.create(new InetSocketAddress(49180), 0);
//            server.createContext("/s5c", new S5RestfulHandler() );
//            server.setExecutor(null); // creates a default executor
//            server.start();
//        }
//		catch (Exception e)
//		{
//
//			e.printStackTrace();
//			logger.error("Failt to start jetty server:", e);
//		}
	}

}
