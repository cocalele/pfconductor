package com.netbric.s5.cli;
import com.bethecoder.ascii_table.ASCIITable;
import com.netbric.s5.cluster.ClusterManager;
import com.netbric.s5.cluster.ZkHelper;
import com.netbric.s5.conductor.*;
import com.netbric.s5.conductor.rpc.*;
import org.apache.commons.cli.*;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CliMain
{

	static final Logger logger = LoggerFactory.getLogger(CliMain.class);
	static final String defaultCfgPath = "/etc/pureflash/pf.conf";
	static String zkBaseDir;
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
		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");

		try
		{
			String cmd_verb = args[0];
			if(cmd_verb.equals("help") || cmd_verb.equals("-h") || cmd_verb.equals("--help"))
			{
				printUsage();
				System.exit(1);
			}
			args = ArrayUtils.remove(args, 0);
			cmd = cp.parse(options, args);
			String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
			Config cfg = new Config(cfgPath);
			String clusterName = cfg.getString("cluster", "name", ClusterManager.defaultClusterName, false);
			zkBaseDir = "/pureflash/"+clusterName;
			switch(cmd_verb)
			{

				case "get_leader_conductor":
				{
					String leader = ZkHelper.getLeaderIp(cfg);
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
					String volumeName = cmd.getOptionValue('v');
					long size = parseNumber(cmd.getOptionValue('s'));
					long rep_num = parseNumber(cmd.getOptionValue('r', "1"));

					CreateVolumeReply r = SimpleHttpRpc.invokeConductor(cfg, "create_volume", CreateVolumeReply.class, "name", volumeName,
							"size", size, "rep_cnt", rep_num);
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
				case "list_disk":
					cmd_list_disk(args, options);
					break;
				case "create_snapshot":
					cmd_create_snapshot(args, options);
					break;
				case "get_pfc":
					cmd_get_pfc(args, options);
					break;
				case "get_conn_str":
					cmd_get_conn_str(args, options);
					break;
				default:
				{
					logger.error("Invalid command:{}", cmd_verb);
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

	    ListVolumeReply r = SimpleHttpRpc.invokeConductor(cfg, "list_volume", ListVolumeReply.class);
	    if(r.retCode == RetCode.OK)
			logger.info("Succeed list_volume");
		else
			throw new IOException(String.format("Failed to list_volume , code:%d, reason:%s", r.retCode, r.reason));
		String [] header = { "Id", "Name", "Size", "RepCount", "Status"};

	    System.out.printf("%d volumes returned.\n", r.volumes.length);
		if(r.volumes.length == 0)
		{
			return;
		}
		String[][] data = new String[r.volumes.length][];
		for(int i=0;i<r.volumes.length;i++) {
				data[i] = new String[]{ Long.toString(r.volumes[i].id), r.volumes[i].name, Long.toString(r.volumes[i].size),
						Integer.toString(r.volumes[i].rep_count), r.volumes[i].status };
		}
		ASCIITable.getInstance().printTable(header, data);

	}
	static void cmd_list_store(String[] args, Options options) throws Exception {
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		Config cfg = new Config(cfgPath);

		ListStoreReply r = SimpleHttpRpc.invokeConductor(cfg, "list_store", ListStoreReply.class);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed list_store");
		else
			throw new IOException(String.format("Failed to list_volume , code:%d, reason:%s", r.retCode, r.reason));
		String [] header = { "Id", "Management IP", "Status"};

		String[][] data = new String[r.storeNodes.size()][];
		for(int i=0;i<r.storeNodes.size();i++) {
			data[i] = new String[]{ Long.toString(r.storeNodes.get(i).id), r.storeNodes.get(i).mngtIp, r.storeNodes.get(i).status };
		}
		ASCIITable.getInstance().printTable(header, data);

	}
	static void cmd_list_disk(String[] args, Options options) throws Exception {
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		Config cfg = new Config(cfgPath);

		ListDiskReply r = SimpleHttpRpc.invokeConductor(cfg, "list_disk",  ListDiskReply.class);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed list_disk");
		else
			throw new IOException(String.format("Failed to list_disk , code:%d, reason:%s", r.retCode, r.reason));
		String [] header = { "Store ID", "uuid",  "Status"};

		String[][] data = new String[r.trays.size()][];
		for(int i=0;i<r.trays.size();i++) {
			data[i] = new String[]{ Long.toString(r.trays.get(i).store_id), r.trays.get(i).uuid, r.trays.get(i).status };
		}
		ASCIITable.getInstance().printTable(header, data);

	}
	static void cmd_create_snapshot(String[] args, Options options) throws Exception {
		options.addOption(buildOption("v", "volume_name", true, true, "Volume name to create snapshot"));
		options.addOption(buildOption("n", "snapshot_name", true, true, "Snapshot name to create"));
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		String volName = cmd.getOptionValue('v');
		String snapName = cmd.getOptionValue('n');
		Config cfg = new Config(cfgPath);

		RestfulReply r = SimpleHttpRpc.invokeConductor(cfg, "create_snapshot",  RestfulReply.class,
				"volume_name", volName, "snapshot_name", snapName);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed create_snapshot");
		else
			throw new IOException(String.format("Failed to create_snapshot , code:%d, reason:%s", r.retCode, r.reason));
	}
	static void cmd_get_pfc(String[] args, Options options) throws Exception {
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		Config cfg = new Config(cfgPath);
		String leader = ZkHelper.getLeaderIp(cfg);
		System.out.println(leader);
	}

	static void cmd_get_conn_str(String[] args, Options options) throws Exception {
		CommandLineParser cp = new DefaultParser();
		CommandLine cmd;
		cmd = cp.parse(options, args);
		String cfgPath = cmd.getOptionValue('c', defaultCfgPath);
		Config cfg = new Config(cfgPath);
		System.out.printf("%s %s %s %s\n",
			cfg.getString("db", "ip", null, true),
			cfg.getString("db", "db_name", null, true),
			cfg.getString("db", "user", null, true),
			cfg.getString("db", "pass", null, true));
	}
}
