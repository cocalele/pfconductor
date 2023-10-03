package com.netbric.s5.cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import com.netbric.s5.orm.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

public class ClusterManager
{

	/**
	 * register self as a conductor node
	 * 
	 * conductor节点，在启动后，在zookeeper的/pureflash/<cluster_name>/conductors/目录下创建EPHEMERAL_SEQUENTIAL类型的节点
	 * ,节点的名字为自己的管理IP。
	 */
	static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
	private static ZooKeeper zk;
	public static ZkHelper zkHelper;
	private static Object locker = new Object();
	public static String zkBaseDir;
	public static final String defaultClusterName = "cluster1";
	public static String myZkNodePath;
	public static String mngtIp;
	public static Hashtable<String, Tray> allTray = new Hashtable<>(257);
	public static Hashtable<String, SharedDisk> allSharedDisk = new Hashtable<>(257);

	public static void registerAsConductor(String managmentIp, String zkIp) throws Exception
	{
		try
		{
			mngtIp = managmentIp;
			zk = new ZooKeeper(zkIp, 50000, new Watcher() {
				@Override
				public void process(WatchedEvent event)
				{
					if (event.getState() == KeeperState.SyncConnected)
					{
						if (event.getType() == EventType.NodeChildrenChanged)
						{
							synchronized (locker)
							{
								locker.notify();
							}

						}

					}
				}
			});
			if (zk.exists("/pureflash", false) == null)
			{
				zk.create("/pureflash", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists(zkBaseDir, false) == null)
			{
				zk.create(zkBaseDir, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			zkHelper = new ZkHelper(zk);
			zkHelper.createZkNodeIfNotExist(zkBaseDir + "/conductors",null);
			myZkNodePath = zk.create(ClusterManager.zkBaseDir + "/conductors/conductor", managmentIp.getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			logger.info("Register on ZK as node:{}", myZkNodePath);
		}
		catch (IOException | KeeperException | InterruptedException e)
		{
			throw e;
		}

	}

	public static void unregister()
	{
		try {
			logger.info("Unregister conductor:{}, {}", mngtIp, myZkNodePath);
			zk.delete(myZkNodePath, -1);
			zk.close();
			zk=null;
			zkHelper.zk = null;
			zkHelper = null;
		} catch (InterruptedException |KeeperException e) {
			logger.error("Error to delete node:{}, for:{}", myZkNodePath, e);
		}
	}
	/**
	 * wait until self become the master conductor
	 * 
	 * @param managmentIp
	 */
	public static void waitToBeMaster(String managmentIp)
	{
		try
		{
			synchronized (locker)
			{
				List<String> list = zk.getChildren(zkBaseDir + "/conductors", true);
				String[] nodes = list.toArray(new String[list.size()]);
				Arrays.sort(nodes);
				while (true)
				{
					String leader = new String(zk.getData(zkBaseDir + "/conductors/" + nodes[0], true, null));
					if(leader.equals(managmentIp))
						break;
					logger.info("the master is {}, not me, waiting...",	leader);
					locker.wait();
					list = zk.getChildren(zkBaseDir + "/conductors", true);
					nodes = list.toArray(new String[list.size()]);
					Arrays.sort(nodes);
				}
			}
		}
		catch (KeeperException e)
		{
			logger.error("Zookeeper Exception:" + e);
		}
		catch (InterruptedException e)
		{
			logger.error("Interrupted Exception:" + e);
		}
	}



	public static void updateStoresFromZk()
	{
		try {
			List<String> nodes = zk.getChildren(zkBaseDir + "/stores", null);
			for(String n : nodes)
			{
				updateStoreFromZk(Integer.parseInt(n));
			}
			validateStores();
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed access zk",e);
		}

	}
	public static void updateStoreFromZk(int id) {
		String path = String.format(zkBaseDir + "/stores/%d", id);

		StoreNode n = new StoreNode();
		try {
			n.mngtIp = new String(zk.getData(path+"/mngt_ip", false, null));
		} catch (KeeperException | InterruptedException e ) {
			logger.error("Failed update store from ZK:", e);
			return;
		}
		n.id=id;

		try {
			if(zk.exists(zkBaseDir + "/stores/"+id+"/alive", false) == null)
				n.status = Status.OFFLINE;
			else
				n.status = Status.OK;
		} catch (KeeperException |InterruptedException e) {
			logger.error("Failed update store from ZK:", e);
		}
		if(S5Database.getInstance().sql("select count(*) from t_store where id=?", id).first(long.class) == 0)
			S5Database.getInstance().insert(n);
		else
			S5Database.getInstance().update(n);
		updateStoreTrays(id);
		updateStorePorts(id);
	}
	public static void updateStoreTrays(int  store_id)
	{
		try {
			String trayOnZk = zkBaseDir + "/stores/"+store_id+"/trays";

			logger.info("watchNewChild on: {} ", trayOnZk);
			zkHelper.watchNewChild(trayOnZk, new ZkHelper.NewChildCallback() {
				@Override
				void onNewChild(String childPath) {
					logger.info("New disk found in zk:{}", childPath);
					updateStoreTrays(store_id);
				}
			});
			List<String> trays = zk.getChildren(trayOnZk, null);
			S5Database.getInstance().sql("update t_tray set status=? where status=? and store_id=?", Status.OFFLINE, Status.OK, store_id).execute();
			for(String t : trays)
			{

				Tray tr = new Tray();
				tr.uuid = t;
				if ( zk.exists(trayOnZk+"/"+t+"/online", null) == null)
					tr.status = Status.OFFLINE;
				else
					tr.status = Status.OK;
				logger.info("{} {}", t, tr.status);
				tr.store_id =store_id;
				tr.device = new String(zk.getData(zkBaseDir + "/stores/"+store_id+"/trays/"+t+"/devname", false, null));
				tr.raw_capacity = Long.parseLong(new String(zk.getData(zkBaseDir + "/stores/"+store_id+"/trays/"+t+"/capacity", false, null)));
				tr.object_size =  Long.parseLong(new String(zk.getData(zkBaseDir + "/stores/"+store_id+"/trays/"+t+"/object_size", false, null)));
				zkHelper.watchNode(zkBaseDir + "/stores/"+store_id+"/trays/"+t+"/online", new ZkHelper.NodeChangeCallback() {
					@Override
					void onNodeCreate(String childPath) {
						logger.info("ssd online, {}", childPath);
						S5Database.getInstance().sql("update t_tray set status=? where uuid=?", Status.OK, t).execute();
					}

					@Override
					void onNodeDelete(String childPath) {
						logger.info("ssd offline, {}", childPath);
						S5Database.getInstance().sql("update t_tray set status=? where uuid=?", Status.OFFLINE, t).execute();

					}
				});
				if(S5Database.getInstance().sql("select count(*) from t_tray where uuid=?", t).first(long.class) == 0)
					S5Database.getInstance().insert(tr);
				else
					S5Database.getInstance().update(tr);
				allTray.put(tr.uuid, tr);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed update tray from zk",e);
		}

	}

	public static void updateStorePorts(int  store_id)
	{
		try {
			for(int i=0;i<2;i++) {
				String path = String.format(zkBaseDir + "/stores/%d/%s", store_id, i==0 ?"ports":"rep_ports");
				List<String> ports = zk.getChildren(path, null);
				for(String ip : ports)
				{
					Port p = new Port();
					p.ip_addr = ip;
					p.store_id = store_id;
					p.name = ip;
					p.purpose = i;
					p.status = Status.OK;

					S5Database.getInstance().upsert(p);
					logger.info("upsert port:{}, purpose:{}", p.ip_addr, p.purpose);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed update tray from zk",e);
		}

	}

	static class AliveWatchCbk extends ZkHelper.NodeChangeCallback {
		int id;
		public AliveWatchCbk(int id)
		{
			this.id=id;
		}
		@Override
		void onNodeCreate(String childPath) {
			logger.info("{} created", childPath);
			updateStoreFromZk(id);
		}

		@Override
		void onNodeDelete(String childPath) {
			logger.error("{} removed", childPath);
			S5Database.getInstance().sql("update t_store set status=? where id=?", Status.OFFLINE, id).execute();
		}
	}
	public static void watchStores()
	{
		try {
			List<String> nodes = zk.getChildren(zkBaseDir + "/stores", null);
			for(String n : nodes)
			{
				zkHelper.watchNode(zkBaseDir + "/stores/" + n + "/alive", new AliveWatchCbk(Integer.parseInt(n)));
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed access zk",e);
		}

		zkHelper.watchNewChild(zkBaseDir + "/stores", new ZkHelper.NewChildCallback() {
			@Override
			void onNewChild(String childPath) {
				logger.info("new store found on zk: {}", childPath);
				int id = Integer.parseInt(childPath.substring(childPath.lastIndexOf('/')+1));
				zkHelper.watchNode(childPath + "/alive", new AliveWatchCbk(id) );

			}
		});

	}

	/**
	 * get list of all alive store
	 * 
	 * @return
	 */
	static public List<StoreNode> getAliveStore()
	{
		return S5Database.getInstance().where("status=?", Status.ONLINE).results(StoreNode.class);
	}

	static public void validateStores()
	{
		List<StoreNode> stores = getAliveStore();
		HashMap<String, StoreNode> existing = new HashMap<>();
		for(StoreNode n : stores) {
			if(existing.containsKey(n.mngtIp)) {
				StoreNode old = existing.get(n.mngtIp);
				logger.error("Managment IP:{} duplicated for store node id:{} and {}", n.mngtIp, n.id, old.id);
			} else {
				existing.put(n.mngtIp, n);
			}
		}

	}

	public static void watchSharedDisks()
	{

		zkHelper.watchNewChild(zkBaseDir + "/shared_disks", new ZkHelper.NewChildCallback() {
			@Override
			void onNewChild(String childPath) {
				logger.info("new shared disk found on zk: {}", childPath);
				try {
					updateSharedDiskFromZk(childPath.substring(childPath.lastIndexOf('/')+1, childPath.length()));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		});

	}
	public static void updateSharedDisksFromZk()
	{
		try {
			List<String> diskUuids = zk.getChildren(zkBaseDir + "/shared_disks", null);
			for(String id : diskUuids) {
				updateSharedDiskFromZk(id);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed access zk",e);
		}

	}

	public static void updateSharedDiskFromZk(String uuid) throws InterruptedException {

		String diskPath = zkBaseDir + "/shared_disks/"+uuid;

		byte[] capBytes = null;
		byte[] objSizeBytes = null;
		for(int i=0;i<20;i++){
			try{
				capBytes = zk.getData(diskPath +"/capacity", false, null);
				objSizeBytes = zk.getData(diskPath+"/object_size", false, null); //the last node create by store
				break;
			} catch (KeeperException e ) {
				Thread.sleep(100);
				continue;

			}
		}
		if(capBytes == null || objSizeBytes == null){
			logger.error("Failed to get capacity or object_size for shared disk:{}", uuid);
			return;
		}
		SharedDisk d = new SharedDisk();

		d.raw_capacity = Long.parseLong(new String(capBytes));
		d.object_size =  Long.parseLong(new String(objSizeBytes));


		d.uuid=uuid;
		d.status = Status.OK;

		try {
			List<String> storeIds = zk.getChildren(diskPath + "/stores", null);
			d.coowner = StringUtils.join(storeIds.listIterator(), ',');
			for(String id : storeIds){
				String dev=new String(zk.getData(diskPath + "/stores/"+id+"/dev_name", false, null));
				logger.debug("disk:{} device:{}", d.uuid, dev);
				d.devName.put(id, dev);
			}
		} catch (KeeperException e) {
			logger.error("Failed update sharedisk from ZK:", e);
		}
		S5Database.getInstance().upsert(d);
		logger.info("Find shared disk:{}", d.uuid);
		allSharedDisk.put(d.uuid, d);
	}

	public static String getDiskDeviceName(String diskUuid, String clientStoreId)
	{
		Tray tr = ClusterManager.allTray.get(diskUuid);
		if(tr != null){
			return tr.device;
		} else {
			SharedDisk sd = ClusterManager.allSharedDisk.get(diskUuid);
			logger.debug("Get dev name for disk:{} clientId:{} rst:{}", diskUuid, clientStoreId, sd == null ? "NULL" : sd.devName.get(clientStoreId));
			if(sd != null){
				return sd.devName.get(clientStoreId);
			}
		}
		return null;
	}
}
