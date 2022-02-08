package com.netbric.s5.conductor;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.netbric.s5.conductor.handler.DebugHandler;
import com.netbric.s5.conductor.handler.S5RestfulHandler;
import com.netbric.s5.orm.S5Database;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
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
	private static Server httpServer;

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


			managmentIp = cfg.getString("conductor" , "mngt_ip", managmentIp);
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

			// Start the server
			httpServer = new Server(49180);
			// Add a single handler on context "/hello"
			ContextHandler context = new ContextHandler();
			context.setContextPath("/s5c");
			context.setHandler(new S5RestfulHandler());

			ContextHandlerCollection hc = new ContextHandlerCollection();
			hc.addHandler(context);

			ContextHandler dbgCtx = new ContextHandler();
			dbgCtx.setContextPath("/debug");
			dbgCtx.setHandler(new DebugHandler());
			hc.addHandler(dbgCtx);
			httpServer.setHandler(hc);

			httpServer.start();
			logger.info("HTTP started on port 49180");
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
			httpServer.destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(-1);
	}
}
