package com.netbric.s5.cli;
import com.bethecoder.ascii_table.ASCIITable;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.conductor.Config;
import com.netbric.s5.conductor.ConfigException;
import com.netbric.s5.conductor.RestfulReply;
import com.netbric.s5.conductor.RetCode;
import com.netbric.s5.conductor.rpc.CreateVolumeReply;
import org.apache.commons.cli.*;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CliMain
{

	static final Logger logger = LoggerFactory.getLogger(CliMain.class);
	static String[] validCmds = {"get_leader_conductor", "create_volume", "list_volume" };
	private static void printUsage()
	{
		System.out.println("Usage: pfcli <command> [options]");
		System.out.println("Valid command can be:");
		for(String cmd : validCmds)
			System.out.printf("       %s", cmd);

	}

	private static Option buildOption(String opt, String longOpt, boolean hasArg, boolean required, String description)
	{
		Option.Builder b = Option.builder(opt);
		b.longOpt(longOpt).hasArg(hasArg).desc(description).required(required);
		return b.build();
	}

	private static long parseNumber(String str)
	{
		if(str.length() == 1)
			return Long.parseLong(str);
		long l = Long.parseLong(str.substring(0, str.length() - 1));
		switch(str.charAt(str.length()-1)){
			case 'k':
			case 'K':
				return l << 10;
			case 'm':
			case 'M':
				return l <<20;
			case 'g':
			case 'G':
				return l <<30;
			case 't':
			case 'T':
				return l <<40;
		}
		return Long.parseLong(str);
	}
	public static void main(String[] args)
	{
		Options options = new Options();
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;

		// add t option
		options.addOption("c", true, "s5 config file path");
		options.addOption("h", "help", false, "conductor node index");
		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");

		try
		{
			if(args[0].equals("help") || args[0].equals("-h")  || args[0].equals("--help") ) {
				printUsage();
				System.exit(1);
			}

			if(args[0].equals("get_leader_conductor")) {
				args = ArrayUtils.remove(args, 0);
				cmd = cp.parse(options, args);
				String cfgPath = cmd.getOptionValue('c', "/etc/s5/s5.conf");
				Config cfg = new Config(cfgPath);
				String leader = getLeaderIp(cfg);
				System.out.println(leader);
			}
			else if(args[0].equals("create_volume")) {
				options.addOption(buildOption("v", "name", true, true, "Volume name to create"));
				options.addOption(buildOption("s", "size", true, true, "Volume size"));
				args = ArrayUtils.remove(args, 0);
				cmd = cp.parse(options, args);
				String cfgPath = cmd.getOptionValue('c', "/etc/s5/s5.conf");
				String volumeName = cmd.getOptionValue('v');
				long size = parseNumber(cmd.getOptionValue('s'));
				Config cfg = new Config(cfgPath);
				String leader = getLeaderIp(cfg);
				GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
				Gson gson = builder.create();


				org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
				client.start();
				String url = String.format("http://%s:49180/s5c/?op=create_volume&name=%s&size=%d",
						leader, URLEncoder.encode(volumeName, StandardCharsets.UTF_8.toString()), size);
				logger.info("Send request:{}", url);
				ContentResponse response = client.newRequest(url)
						.method(org.eclipse.jetty.http.HttpMethod.GET)
						.send();
				logger.info("Get response:{}", response.getContentAsString());
				if(response.getStatus() < 200 || response.getStatus() >= 300)
				{
					throw new IOException(String.format("Failed to create_volume:%s, HTTP status:%d, reason:%s",
							volumeName, response.getStatus(), response.getReason()));
				}
				CreateVolumeReply r = gson.fromJson(new String(response.getContent()), CreateVolumeReply.class);
				client.stop();
				if(r.retCode == RetCode.OK)
					logger.info("Succeed create_volume:{}", volumeName);
				else
					throw new IOException(String.format("Failed to create_volume:%s , code:%d, reason:%s", volumeName, r.retCode, r.reason));
				String [] header = { "Id", "Name", "Size", "RepCount", "Status"};

				String[][] data = {
						{ Long.toString(r.id), r.name, Long.toString(r.size), Integer.toString(r.rep_count), r.status },

				};
				ASCIITable.getInstance().printTable(header, data);

				return;
			}
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			logger.error("Failed: {}", e1.getMessage());
			System.exit(1);
		}

    }

	private static String getLeaderIp(Config cfg) throws ConfigException, IOException, KeeperException, InterruptedException {
		String zkIp = cfg.getString("zookeeper", "ip", null, true);
		if(zkIp == null)
		{
			System.err.println("zookeeper ip not specified in config file");
			System.exit(1);
		}
		ZooKeeper zk = new ZooKeeper(zkIp, 50000, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				logger.info("ZK event:{}", event.toString());
			}
		});
		List<String> list = zk.getChildren("/s5/conductors", false);
		if(list.size() == 0){
			logger.error("No active conductor found on zk:{}", zkIp);
			System.exit(1);
		}
		String[] nodes = list.toArray(new String[list.size()]);
		Arrays.sort(nodes);
		String leader = new String(zk.getData("/s5/conductors/" + nodes[0], true, null));
		logger.info("Get leader conductor:{}", leader);
		return leader;
	}

}
