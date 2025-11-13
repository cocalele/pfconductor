package com.netbric.s5.cli;
import com.bethecoder.ascii_table.ASCIITable;
import com.netbric.s5.cluster.ClusterManager;
import com.netbric.s5.cluster.ZkHelper;
import com.netbric.s5.conductor.*;
import com.netbric.s5.conductor.rpc.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import com.netbric.s5.conductor.handler.StoreHandler.*;

public class CliMain
{

	static final Logger logger = LoggerFactory.getLogger(CliMain.class);
	static final String defaultCfgPath = "/etc/pureflash/pf.conf";
	static String zkBaseDir;
	static String[][] validCmds = {
			{"get_pfc", "", "Get the active conductor IP"},
			{"create_volume", " -v <vol_name> -s <size> [-r <replica_num>]", "create a volume"},
			{"list_volume", "", "list volumes"}
	};
	private static void printUsage()
	{
		System.out.println("Usage: pfcli <command> [options]");
		System.out.println("Valid command can be:");
		for(String[] cmd : validCmds)
			System.out.printf("       %s %s\n\t%s\n", cmd[0], cmd[1], cmd[2]);

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
	interface CmdRunner {
		void run(Namespace cmd, Config cfg) throws Exception;
	}
	public static void main(String[] args)
	{
//		Options options = new Options();
//		CommandLineParser cp = new DefaultParser();
//		CommandLine cmd;
		try
		{
			ArgumentParser cp = ArgumentParsers.newFor("pfcli").build()
					.description("PureFlash command line tool");
			cp.addArgument("-c").metavar("cfg_file_path").required(false).help("pfs config file path").setDefault(defaultCfgPath);
			Subparsers sps = cp.addSubparsers().dest("cmd_verb");
//			ArrayList<String> remainArgs = new ArrayList<>();
//			Namespace ns = cp.parseKnownArgs(args, remainArgs);

			System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");



			Subparser sp = sps.addParser("get_leader_conductor");
			sp.description("Get leader pfconductor IP");

			sp = sps.addParser("handle_error");
			sp.description("Handle error case when some replicas are in error state");
			sp.addArgument("-i").help("Replica id").required(true).type(Integer.class).metavar("rep_id");
			sp.addArgument("-sc").help("State code, 0xC0 - 0xCA, e.g.: 0xC0 means MSG_STATUS_NOT_PRIMARY").required(true).metavar("sc");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_handle_error(cmd, cfg);
				}
			});

			sp = sps.addParser("create_volume");
			sp.description("Create volume");
			sp.addArgument("-v").help("Volume name to create").required(true).metavar("volume_name");
			sp.addArgument("-s").help("Volume size, unit like GB is accepted").required(true).metavar("size");
			sp.addArgument("-r", "--rep_num").help("Replica number").type(Integer.class).setDefault(1).metavar("rep_num");
			sp.addArgument("--host_id").help("Host ID to affinity primary").type(Integer.class).setDefault(-1).metavar("host_id");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_create_volume(cmd, cfg);
				}
			});

			sp = sps.addParser("delete_volume");
			sp.description("Delete volume")
				.addArgument("-v").help("Volume name to delete").required(true).metavar("volume_name");
			sp.setDefault("__func", (CmdRunner) (cmd, cfg) ->{
					String volumeName = cmd.getString("v");

					CreateVolumeReply r = SimpleHttpRpc.invokeConductor(cfg, "delete_volume", CreateVolumeReply.class, "volume_name", volumeName);
					if(r.retCode == RetCode.OK)
						logger.info("Succeed delete volume:{}", volumeName);
					else
						throw new IOException(String.format("Failed to delete volume:%s , code:%d, reason:%s", volumeName, r.retCode, r.reason));
				});

			sp=sps.addParser("list_volume");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_list_volume(cmd, cfg);
				}
			});

			sp=sps.addParser("list_store");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_list_store(cmd, cfg);
				}
			});

			sp=sps.addParser("list_port");
			sp.description("List port info");
			sp.addArgument("-i").help("Node Id name to check the port").required(true).metavar("node_id");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_list_port(cmd, cfg);
				}
			});

			sp=sps.addParser("list_disk");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_list_disk(cmd, cfg);
				}
			});

			sp=sps.addParser("create_snapshot");
			sp.addArgument("-v").help("Volume name to create snapshot").required(true).metavar("volume_name");
			sp.addArgument("-n").help("Name of snapshot").required(true).metavar("snap_name");
			sp.setDefault("__func", (CmdRunner) CliMain::cmd_create_snapshot);

			sp=sps.addParser("delete_snapshot");
			sp.addArgument("-v").help("Volume name to delete snapshot").required(true).metavar("volume_name");
			sp.addArgument("-n").help("Name of snapshot").required(true).metavar("snap_name");
			sp.setDefault("__func", (CmdRunner) CliMain::cmd_delete_snapshot);

			sp=sps.addParser("get_pfc");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_get_pfc(cmd, cfg);
				}
			});

			sp=sps.addParser("get_conn_str");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_get_conn_str(cmd, cfg);
				}
			});

			sp=sps.addParser("scrub_volume");
			sp.addArgument("-v").help("Volume name to scrub").required(true).metavar("volume_name");
			//sp.addArgument("-t").help("Tenant name of volume").required(true).metavar("tenant_name");
			sp.setDefault("__func", (CmdRunner) CliMain::cmd_scrub_volume);

			sp=sps.addParser("recovery_volume");
			sp.addArgument("-v").help("Volume name to scrub").required(true).metavar("volume_name");
			//sp.addArgument("-t").help("Tenant name of volume").required(true).metavar("tenant_name");
			sp.setDefault("__func", (CmdRunner) CliMain::cmd_recovery_volume);

			sp = sps.addParser("create_pfs2");
			sp.description("Create PFS2 file");
			sp.addArgument("-v").help("File name to create").required(true).metavar("volume_name");
			sp.addArgument("-s").help("File size, unit like GB is accepted").required(true).metavar("size");
			sp.addArgument("-d").help("Shared disk name to create file on").required(true).metavar("dev_name");
			sp.setDefault("__func", new CmdRunner() {
				@Override
				public void run(Namespace cmd, Config cfg) throws Exception {
					cmd_create_pfs2(cmd, cfg);
				}
			});

			Namespace cmd = cp.parseArgs(args);
			String cfgPath = cmd.getString("c");
			Config cfg = new Config(cfgPath);
			String clusterName = cfg.getString("cluster", "name", ClusterManager.defaultClusterName, false);
			zkBaseDir = "/pureflash/"+clusterName;


			String cmd_verb = cmd.getString("cmd_verb");
			switch(cmd_verb)
			{
				case "get_leader_conductor":
				{
					String leader = ZkHelper.getLeaderIp(cfg);
					System.out.println(leader);
					break;
				}
				default:
					((CmdRunner)cmd.get("__func")).run(cmd, cfg);
			}
		}
		catch(HelpScreenException e){
			return ;
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			logger.error("Failed: {}", e1.getMessage());
			System.exit(1);
		}

    }

	private static void cmd_handle_error(Namespace cmd, Config cfg) throws Exception {
		long repId = cmd.getInt("i");
		System.out.println("repId: " + repId);
		long stateCode = Long.decode(cmd.getString("sc"));
		System.out.println("stateCode: " + stateCode);
		
		RestfulReply r = SimpleHttpRpc.invokeConductor(cfg, "handle_error", RestfulReply.class,
				"rep_id", repId,
				"sc", stateCode);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed handle_error for replica id:{}", repId);
		else
			throw new IOException(String.format("Failed to handle_error for replica id:%d , code:%d, reason:%s", repId, r.retCode, r.reason));
	}

	private static void cmd_create_volume(Namespace cmd, Config cfg) throws Exception {
		String volumeName = cmd.getString("v");
		long size = parseNumber(cmd.getString("s"));
		long rep_num =cmd.getInt("rep_num");
		long hostId = cmd.getInt("host_id");
		CreateVolumeReply r = SimpleHttpRpc.invokeConductor(cfg, "create_volume", CreateVolumeReply.class, "volume_name", volumeName,
				"size", size, "rep_cnt", rep_num, "host_id", hostId);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed create_volume:{}", volumeName);
		else
			throw new IOException(String.format("Failed to create_volume:%s , code:%d, reason:%s", volumeName, r.retCode, r.reason));
		String [] header = { "Id", "Name", "Size", "RepCount", "Status"};

		String[][] data = {
				{ Long.toString(r.id), r.name, Long.toString(r.size), Integer.toString(r.rep_count), r.status },

		};
		ASCIITable.getInstance().printTable(header, data);
	}

	private static String readDiskUuid(String devName) throws IOException {
		//the first bytes of a disk
//		struct HeadPage {
//		uint32_t magic;
//		uint32_t version;
//		unsigned char uuid[ 16];
//		}
//		head.magic = 0x3553424e; //magic number, NBS5
//		head.version= S5_VERSION; //S5 version #define S5_VERSION 0x00030000

		FileInputStream fi = new FileInputStream(devName);
		byte[] buf = new byte[32];
		int cnt = fi.read(buf);
		if (!Arrays.equals(new byte[]{0x4e, 0x42, 0x53, 0x35}, 0, 4, buf, 0, 4)) {
			throw new IOException("Magic number mismatch, not a valid pfbd disk");
		}
		if (!Arrays.equals(new byte[]{0x0, 0x0, 0x03, 0x0}, 0, 4, buf, 4, 8)) {
			throw new IOException("Version number mismatch, not a valid pfbd disk");
		}

		String uuid = unpareUuid(buf, 8);
		return uuid;
	}

	private static String unpareUuid(byte[] buf, int startIndex)
	{
		StringBuilder sb = new StringBuilder(32);
		//b4a67579-2ee3-4317-8163-8fccf77d77fd
		for(int i=0;i<4;i++){
			sb.append(String.format("%02x", buf[startIndex++]));
		}
		sb.append("-");
		for(int i=0;i<2;i++){
			sb.append(String.format("%02x", buf[startIndex++]));
		}
		sb.append("-");
		for(int i=0;i<2;i++){
			sb.append(String.format("%02x", buf[startIndex++]));
		}
		sb.append("-");
		for(int i=0;i<2;i++){
			sb.append(String.format("%02x", buf[startIndex++]));
		}
		sb.append("-");
		for(int i=0;i<6;i++){
			sb.append(String.format("%02x", buf[startIndex++]));
		}
		return sb.toString();
	}
	private static void cmd_create_pfs2(Namespace cmd, Config cfg) throws Exception {
		String volumeName = cmd.getString("v");
		long size = parseNumber(cmd.getString("s"));
		String diskName =cmd.getString("d");
		String uuid = readDiskUuid(diskName);
		logger.info("Create PFS2 file on disk:{} uuid:{}", diskName, uuid);

		Config pfsCfg = new Config("/etc/pureflash/pfs.conf");
		String storeId = pfsCfg.getString("afs", "id", null, true);
		CreateVolumeReply r = SimpleHttpRpc.invokeConductor(cfg, "create_pfs2", CreateVolumeReply.class, "volume_name", volumeName,
				"size", size, "dev_uuid", uuid, "client_id", storeId);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed create PFS2 file:{}", volumeName);
		else
			throw new IOException(String.format("Failed to create_volume:%s , code:%d, reason:%s", volumeName, r.retCode, r.reason));
		String [] header = { "Id", "Name", "Size", "RepCount", "Status"};

		String[][] data = {
				{ Long.toString(r.id), r.name, Long.toString(r.size), Integer.toString(r.rep_count), r.status },

		};
		ASCIITable.getInstance().printTable(header, data);
	}

	static void cmd_list_volume(Namespace cmd, Config cfg) throws Exception {
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
	static void cmd_list_store(Namespace cmd, Config cfg) throws Exception {
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
	static void cmd_list_port(Namespace cmd, Config cfg) throws Exception {
		String id = cmd.getString("i");
		System.out.println("id:" + id);
		ListNodePortReply r = SimpleHttpRpc.invokeConductor(cfg, "list_port", ListNodePortReply.class,
						"id", id);
		if(r.retCode == RetCode.OK)
		logger.info("Succeed list_port");
		else
			throw new IOException(String.format("Failed to list_port , code:%d, reason:%s", r.retCode, r.reason));
		String [] header = { "Name", "IP Address", "Store Id", "Purpose"}; // "Status",

		String[][] data = new String[r.ports.length][];
		for (int i = 0; i < r.ports.length; i++) {
			data[i] = new String[]{
				r.ports[i].name,
				r.ports[i].ip_addr,
				Long.toString(r.ports[i].store_id),
				Long.toString(r.ports[i].purpose),
				// r.ports[i].status
			};
		}
		ASCIITable.getInstance().printTable(header, data);
	}
	static void cmd_list_disk(Namespace cmd, Config cfg) throws Exception {
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
	static void cmd_create_snapshot(Namespace cmd, Config cfg) throws Exception {
		String volName = cmd.getString("v");
		String snapName = cmd.getString("n");

		RestfulReply r = SimpleHttpRpc.invokeConductor(cfg, "create_snapshot",  RestfulReply.class,
				"volume_name", volName, "snapshot_name", snapName);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed create_snapshot");
		else
			throw new IOException(String.format("Failed to create_snapshot , code:%d, reason:%s", r.retCode, r.reason));
	}
	static void cmd_delete_snapshot(Namespace cmd, Config cfg) throws Exception {
		String volName = cmd.getString("v");
		String snapName = cmd.getString("n");

		RestfulReply r = SimpleHttpRpc.invokeConductor(cfg, "delete_snapshot",  RestfulReply.class,
				"volume_name", volName, "snapshot_name", snapName);
		if(r.retCode == RetCode.OK)
			logger.info("Succeed create_snapshot");
		else
			throw new IOException(String.format("Failed to create_snapshot , code:%d, reason:%s", r.retCode, r.reason));
	}

	static void cmd_scrub_volume(Namespace cmd, Config cfg) throws Exception {
		String volName = cmd.getString("v");

		BackgroundTaskReply r = SimpleHttpRpc.invokeConductor(cfg, "scrub_volume",  BackgroundTaskReply.class,
				"volume_name", volName);
		if(r.retCode != RetCode.OK)
			throw new IOException(String.format("Failed to scrub volume , code:%d, reason:%s", r.retCode, r.reason));

		for(;r.task.status == BackgroundTaskManager.TaskStatus.RUNNING || r.task.status == BackgroundTaskManager.TaskStatus.WAITING; Thread.sleep(2000)) {
			r = SimpleHttpRpc.invokeConductor(cfg, "query_task", BackgroundTaskReply.class,
					"task_id", r.taskId);
			if (r.retCode != RetCode.OK)
				throw new IOException(String.format("Failed to scrub volume , code:%d, reason:%s", r.retCode, r.reason));
			logger.info("{}, status:{}, progress:{}", r.task.desc, r.task.status, r.task.progress);
		}

		logger.info("Scrub volume:{} complete, status:{}", volName, r.task.status);

	}

	static void cmd_recovery_volume(Namespace cmd, Config cfg) throws Exception {
		String volName = cmd.getString("v");

		BackgroundTaskReply r = SimpleHttpRpc.invokeConductor(cfg, "recovery_volume",  BackgroundTaskReply.class,
				"volume_name", volName);
		if(r.retCode != RetCode.OK)
			throw new IOException(String.format("Failed to reocvery volume , code:%d, reason:%s", r.retCode, r.reason));

		for(;r.task.status == BackgroundTaskManager.TaskStatus.RUNNING || r.task.status == BackgroundTaskManager.TaskStatus.WAITING; Thread.sleep(2000)) {
			r = SimpleHttpRpc.invokeConductor(cfg, "query_task", BackgroundTaskReply.class,
					"task_id", r.taskId);
			if (r.retCode != RetCode.OK)
				throw new IOException(String.format("Failed to recovery volume , code:%d, reason:%s", r.retCode, r.reason));
			logger.info("{}, status:{}, progress:{}", r.task.desc, r.task.status, r.task.progress);
		}

		logger.info("Recovery volume:{} complete, status:{}", volName, r.task.status);

	}

	static void cmd_get_pfc(Namespace cmd, Config cfg) throws Exception {
		String leader = ZkHelper.getLeaderIp(cfg);
		System.out.println(leader);
	}

	static void cmd_get_conn_str(Namespace cmd, Config cfg) throws Exception {
		System.out.printf("%s %s %s %s\n",
			cfg.getString("db", "ip", null, true),
			cfg.getString("db", "db_name", null, true),
			cfg.getString("db", "user", null, true),
			cfg.getString("db", "pass", null, true));
	}
}
