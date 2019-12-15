package com.netbric.s5.cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import com.netbric.s5.orm.S5Database;
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

import com.netbric.s5.orm.StoreNode;

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
				while (!(new String(zk.getData("/s5/conductors/" + nodes[0], true, null))).equals(managmentIp))
				{
					logger.info("the master is {}, not me, waiting...",
							new String(zk.getData("/s5/conductors/" + nodes[0], true, null)));
					locker.wait();
					list = zk.getChildren("/s5", true);
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

	public static  void watchStoreAlive(String mngtIp,
												java.util.function.BiConsumer<EventType, String> func) {
		new Thread(()->{
			try {
				String path = String.format("/s5/stores/%s/alive", mngtIp);
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
			List<String> nodes = zk.getChildren("/s5/stores", null);
			for(String n : nodes)
			{
				updateStoreFromZk(n);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Failed access zk",e);
		}

	}
	public static void updateStoreFromZk(String ip)
	{
		String path = String.format("/s5/stores/%s", ip);

		StoreNode n = new StoreNode();
		n.mngtIp = ip;
		n.status = StoreNode.STATUS_OK;
		if(S5Database.getInstance().sql("select count(*) from t_s5store where mngt_ip=?", ip).first(long.class) == 0)
			S5Database.getInstance().insert(n);
		else
			S5Database.getInstance().update(n);
	}
	public static void watchStores()
	{
//		BiConsumer<org.apache.zookeeper.Watcher.Event.EventType, String> c =
		try {
			watchNodeForChildChange("/s5/stores", (EventType evt, String path) -> {
				logger.info("{} on path: {}", evt, path);
				if(evt == EventType.NodeCreated)
				{
					String mngtIp = path.substring(path.lastIndexOf('/')+1);

					watchStoreAlive(mngtIp, (EventType evt2, String path2)->{});

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
