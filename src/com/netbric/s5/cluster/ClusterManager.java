package com.netbric.s5.cluster;

import java.io.IOException;
import java.util.Arrays;
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

import static org.apache.zookeeper.Watcher.Event.EventType.NodeCreated;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

public class ClusterManager
{

	/**
	 * register self as a conductor node
	 * 
	 * conductor节点，在启动后，在zookeeper的/s5/conductors/目录下创建EPHEMERAL_SEQUENTIAL类型的节点
	 * ,节点的名字为自己的管理IP。
	 */
	static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
	private static ZooKeeper zk;
	private static Object locker = new Object();

	public static void registerAsConductor(String managmentIp, String zkIp) throws Exception
	{
		try
		{
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
			if (zk.exists("/s5", false) == null)
			{
				zk.create("/s5", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			createZkNodeIfNotExist("/s5/conductors",null);
			zk.create("/s5/conductors/conductor", managmentIp.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		}
		catch (IOException | KeeperException | InterruptedException e)
		{
			throw e;
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
				List<String> list = zk.getChildren("/s5/conductors", true);
				String[] nodes = list.toArray(new String[list.size()]);
				Arrays.sort(nodes);
				while (true)
				{
					String leader = new String(zk.getData("/s5/conductors/" + nodes[0], true, null));
					if(leader.equals(managmentIp))
						break;
					logger.info("the master is {}, not me, waiting...",	leader);
					locker.wait();
					list = zk.getChildren("/s5/conductors", true);
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

	/**
	 * register as a store node
	 * 
	 * 在zookeeper的/s5/stores/目录下创建EPHEMERAL节点，节点的名字为自己的管理IP。
	 * 
	 * @param managmentIp
	 */
	public static void registerAsStore(String managmentIp)
	{
		try
		{
			zk.create("/s5/stores", managmentIp.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
		catch (KeeperException | InterruptedException e)
		{
			logger.error("Interrupted or Zookeerper Exception:" + e);
		}
	}

	public static  void watchNodeForChildChange(String path,
											 java.util.function.BiConsumer<EventType, String> func) throws KeeperException, InterruptedException {
		new Thread(()->{
			try {
				zk.getChildren(path, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						logger.info("ZK event:{} on path:{}", event.getType().toString(), event.getPath());
						if (event.getType() != NodeCreated && event.getType() != NodeDeleted) {
							logger.error("Unexpected zk event:{}", event.getType().toString());
							return;
						}
						func.accept(event.getType(), event.getPath());
					}
				});
			}
			catch (Exception e)
			{
				logger.error("Error during watch zk:", e);
			}
		});

	}

	public static  void watchStoreAlive(int id,
												java.util.function.BiConsumer<EventType, String> func) {
		new Thread(()->{
			try {
				String path = String.format("/s5/stores/%d/alive", id);
				zk.exists(path, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						logger.info("ZK event:{} on path:{}", event.getType().toString(), event.getPath());
						if (event.getType() != NodeCreated && event.getType() != NodeDeleted) {
							logger.error("Unexpected zk event:{}", event.getType().toString());
							return;
						}
						func.accept(event.getType(), event.getPath());
					}
				});
			}
			catch (Exception e)
			{
				logger.error("Error during watch alive:", e);
			}
		});

	}

	public static void updateStoresFromZk()
	{
		try {
		  createZkNodeIfNotExist("/s5/stores", null);
			List<String> nodes = zk.getChildren("/s5/stores", null);
			for(String n : nodes)
			{
				updateStoreFromZk(Integer.parseInt(n));
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed access zk",e);
		}

	}
	public static void updateStoreFromZk(int id)  {
		String path = String.format("/s5/stores/%d", id);

		StoreNode n = new StoreNode();
		try {
			n.mngtIp = new String(zk.getData(path+"/mngt_ip", false, null));
		} catch (KeeperException | InterruptedException e ) {
			logger.error("Failed update store from ZK:", e);
			return;
		}
		n.id=id;
		n.status = StoreNode.STATUS_OK;
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
			List<String> trays = zk.getChildren("/s5/stores/"+store_id+"/trays", null);
			for(String t : trays)
			{

				Tray tr = new Tray();
				tr.uuid = t;
				tr.status = Status.OK;
				tr.store_id =store_id;
				tr.device = new String(zk.getData("/s5/stores/"+store_id+"/trays/"+t+"/devname", false, null));
				tr.raw_capacity = Long.parseLong(new String(zk.getData("/s5/stores/"+store_id+"/trays/"+t+"/capacity", false, null)));
				if(S5Database.getInstance().sql("select count(*) from t_tray where uuid=?", t).first(long.class) == 0)
					S5Database.getInstance().insert(tr);
				else
					S5Database.getInstance().update(tr);

			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed update tray from zk",e);
		}

	}

	public static void updateStorePorts(int  store_id)
	{
		try {
			List<String> ports = zk.getChildren("/s5/stores/"+store_id+"/ports", null);
			for(String ip : ports)
			{
				String purpose = new String(zk.getData("/s5/stores/"+store_id+"/ports/"+ip+"/purpose", false, null));
				Port p = new Port();
				p.ip_addr = ip;
				p.store_id = store_id;
				p.name = ip;
				p.purpose = Integer.parseInt(purpose);
				p.status = Status.OK;

				S5Database.getInstance().upsert(p);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed update tray from zk",e);
		}

	}

	public static void watchStores()
	{
//		BiConsumer<org.apache.zookeeper.Watcher.Event.EventType, String> c =
		try {
			watchNodeForChildChange("/s5/stores", (EventType evt, String path) -> {
				logger.info("{} on path: {}", evt, path);
				if(evt == EventType.NodeCreated)
				{
					int id = Integer.parseInt(path.substring(path.lastIndexOf('/')+1));

					watchStoreAlive(id, (EventType evt2, String path2)->{
						if(evt == EventType.NodeCreated)
						{
							updateStoreFromZk(id);
						}
					});

				}

			});
		}
		catch(Exception e)
		{
			logger.error("Error during watch alive:", e);
		}
	}
	private static void createZkNodeIfNotExist(String path, String data) throws KeeperException, InterruptedException {
		if(zk.exists(path, false) != null)
			return;
		zk.create(path, data == null ? null : data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

	}
	/**
	 * get list of all alive store
	 * 
	 * @return
	 */
	public List<StoreNode> getAliveStore()
	{
		return null;
	}
}
