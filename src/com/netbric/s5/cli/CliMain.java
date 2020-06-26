package com.netbric.s5.cli;
import com.bethecoder.ascii_table.ASCIITable;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.conductor.*;
import com.netbric.s5.conductor.rpc.CreateVolumeReply;
import com.netbric.s5.conductor.rpc.ListStoreReply;
import com.netbric.s5.conductor.rpc.ListVolumeReply;
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
	static final String defaultCfgPath = "/etc/pureflash/pf.conf";
	static String[][] validCmds = {
			{"get_pfc", "", "Get the active conductor IP"},
			{"create_volume", " -v <vol_namej> -s <size> [-r <replica_num>]", "create a volume"},
			{"list_volume", "", "list volumes"}
	};
	private static void printUsage()
	{
		System.out.println("Usage: pfcli <command> [options]");
		System.out.println("Valid command can be:");
		for(String[] cmd : validCmds)
			System.out.printf("       %s %s\n\t%s\n", cmd[0], cmd[1], cmd[2]);

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
		options.addOption("c", true, "s5 config file path, default:"+defaultCfgPath);
		options.addOption("h", "help", false, "conductor node index");
		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");

		try
		{
			switch(args[0])
			{
				case "help":
				case "-h":
				case "--help":
				{
					printUsage();
					System.exit(1);
				}
				case "get_leader_conductor":
				{
					args = ArrayUtils.remove(args, 0);
					cmd = cp.parse(options, args);
					String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
					Config cfg = new Config(cfgPath);
					String leader = getLeaderIp(cfg);
					System.out.println(leader);
					break;
				}
				case "create_volume":
				{
					options.addOption(buildOption("v", "name", true, true, "Volume name to create"));
					options.addOption(buildOption("s", "size", true, true, "Volume size"));
					options.addOption(buildOption("r", "rep_num", true, false, "Replica number, default 1"));
					args = ArrayUtils.remove(args, 0);
					cmd = cp.parse(options, args);
					String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
					String volumeName = cmd.getOptionValue('v');
					long size = parseNumber(cmd.getOptionValue('s'));
					long rep_num = parseNumber(cmd.getOptionValue('r', "1"));
					Config cfg = new Config(cfgPath);
					String leader = getLeaderIp(cfg);
					GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
					Gson gson = builder.create();


					org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
					client.start();
					String url = String.format("http://%s:49180/s5c/?op=create_volume&name=%s&size=%d&rep_cnt=%d",
							leader, URLEncoder.encode(volumeName, StandardCharsets.UTF_8.toString()), size, rep_num);
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
				case "list_volume":
					cmd_list_volume(args, options);
					break;
				case "list_store":
					cmd_list_store(args, options);
					break;
				default:
				{
					logger.error("Invalid command:{}", args[0]);
					return;
				}
			}
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			logger.error("Failed: {}", e1.getMessage());
			System.exit(1);
		}

    }

    static void cmd_list_volume(String[] args, Options options) throws Exception {
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		Config cfg = new Config(cfgPath);
		String leader = getLeaderIp(cfg);
		GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
		Gson gson = builder.create();

		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		client.start();
		String url = String.format("http://%s:49180/s5c/?op=list_volume", leader);
		logger.info("Send request:{}", url);
		ContentResponse response = client.newRequest(url)
				.method(org.eclipse.jetty.http.HttpMethod.GET)
				.send();
		logger.info("Get response:{}", response.getContentAsString());
		if(response.getStatus() < 200 || response.getStatus() >= 300)
		{
			throw new IOException(String.format("Failed to list_volume, HTTP status:%d, reason:%s",
					response.getStatus(), response.getReason()));
		}
		ListVolumeReply r = gson.fromJson(new String(response.getContent()), ListVolumeReply.class);
		client.stop();
		if(r.retCode == RetCode.OK)
			logger.info("Succeed list_volume");
		else
			throw new IOException(String.format("Failed to list_volume , code:%d, reason:%s", r.retCode, r.reason));
		String [] header = { "Id", "Name", "Size", "RepCount", "Status"};

		String[][] data = new String[r.volumes.length][];
		for(int i=0;i<r.volumes.length;i++) {
				data[i] = new String[]{ Long.toString(r.volumes[i].id), r.volumes[i].name, Long.toString(r.volumes[i].size),
						Integer.toString(r.volumes[i].rep_count), r.volumes[i].status };
		};
		ASCIITable.getInstance().printTable(header, data);

	}
	static void cmd_list_store(String[] args, Options options) throws Exception {
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		Config cfg = new Config(cfgPath);
		String leader = getLeaderIp(cfg);
		GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
		Gson gson = builder.create();

		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		client.start();
		String url = String.format("http://%s:49180/s5c/?op=list_store", leader);
		logger.info("Send request:{}", url);
		ContentResponse response = client.newRequest(url)
				.method(org.eclipse.jetty.http.HttpMethod.GET)
				.send();
		logger.info("Get response:{}", response.getContentAsString());
		if(response.getStatus() < 200 || response.getStatus() >= 300)
		{
			throw new IOException(String.format("Failed to list_store, HTTP status:%d, reason:%s",
					response.getStatus(), response.getReason()));
		}
		ListStoreReply r = gson.fromJson(new String(response.getContent()), ListStoreReply.class);
		client.stop();
		if(r.retCode == RetCode.OK)
			logger.info("Succeed list_volume");
		else
			throw new IOException(String.format("Failed to list_volume , code:%d, reason:%s", r.retCode, r.reason));
		String [] header = { "Id", "Management IP", "Status"};

		String[][] data = new String[r.store_nodes.length][];
		for(int i=0;i<r.store_nodes.length;i++) {
			data[i] = new String[]{ Long.toString(r.store_nodes[i].id), r.store_nodes[i].mngt_ip, r.store_nodes[i].status };
		};
		ASCIITable.getInstance().printTable(header, data);

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
