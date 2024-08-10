package com.netbric.s5.conductor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.netbric.s5.conductor.handler.DebugHandler;
import com.netbric.s5.conductor.handler.S5RestfulHandler;
import com.netbric.s5.orm.S5Database;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netbric.s5.conductor.HTTPServer.ContextHandler;
import com.netbric.s5.conductor.HTTPServer.Request;
import com.netbric.s5.conductor.HTTPServer.Response;
import com.netbric.s5.cluster.ClusterManager;

public class Main
{
	static {
		System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy/MM/dd H:mm:ss.SSS]");

	}
	static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static PfcServer httpServer;

	public static void main(String[] args)
	{
		ArgumentParser parser = ArgumentParsers.newFor("pfc").build()
				.description("Pureflash conductor");
		parser.addArgument("-c")
				.metavar("conf")
				.setDefault("/etc/pureflash/pfc.conf")
				.help("config file path");
//		parser.addArgument("-i").type(Integer.class)
//				.metavar("node_idx")
//				.required(true)
//				.help("conductor node index");

		try
		{
			Namespace cmd = parser.parseArgs(args);
			String cfgPath = cmd.getString("c");
			System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
			logger.info("use config file: {}", cfgPath);
			Config cfg = new Config(cfgPath);

			//InetAddress ia = null;
			//try
			//{
			//	ia = InetAddress.getLocalHost(); //may fail in container with --network host
			//}
			//catch (UnknownHostException e1)
			//{
			//	// TODO Auto-generated catch block
			//	e1.printStackTrace();
			//}
			//String managmentIp = ia.getHostAddress();


			String managmentIp = cfg.getString("conductor" , "mngt_ip", null);
			if(managmentIp == null)
			{
				System.err.println("managmentIp ip not specified in config file");
				System.exit(1);
			}
			String zkIp = cfg.getString("zookeeper", "ip", null, true);
			if(zkIp == null)
			{
				System.err.println("zookeeper ip not specified in config file");
				System.exit(1);
			}
			String clusterName = cfg.getString("cluster", "name", ClusterManager.defaultClusterName, false);
			ClusterManager.zkBaseDir = "/pureflash/"+clusterName;
			ClusterManager.registerAsConductor(managmentIp, zkIp);
			ClusterManager.waitToBeMaster(managmentIp);
			S5Database.getInstance().init(cfg);
			ClusterManager.zkHelper.createZkNodeIfNotExist(ClusterManager.zkBaseDir + "/stores", null);
			ClusterManager.watchStores();
			ClusterManager.updateStoresFromZk();
			ClusterManager.zkHelper.createZkNodeIfNotExist(ClusterManager.zkBaseDir + "/shared_disks", null);
			ClusterManager.watchSharedDisks();
			ClusterManager.updateSharedDisksFromZk();

			// Start the server
			httpServer = new PfcServer(49180);

			// Add a single handler on context "/hello"

			//not works
//			ExecutorThreadPool tp = new ExecutorThreadPool(8, 32, 60L, TimeUnit.SECONDS);
//			httpServer = new Server(tp);
//			ServerConnector connector = new ServerConnector(httpServer);
//			connector.setPort(49180);
//			httpServer.setConnectors(new Connector[]{connector});


			httpServer.addContext("/s5c", new S5RestfulHandler());

			httpServer.addContext("/debug", new DebugHandler());
			

			httpServer.start();
			logger.info("HTTP started on port 49180");
			while(true){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					
					System.exit(0);
				}
			}
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			logger.error("Failed to run jconductor:{}", e1);
		}
	}

	public static void suicide()
	{
		ClusterManager.unregister();
		try {
			httpServer.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(-1);
	}
}
