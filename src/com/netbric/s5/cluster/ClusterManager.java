package com.netbric.s5.cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
			zk.create("/s5/conductors", managmentIp.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
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
				List<String> list = zk.getChildren("/s5", true);
				String[] nodes = list.toArray(new String[list.size()]);
				Arrays.sort(nodes);
				while (!(new String(zk.getData("/s5/" + nodes[0], true, null))).equals(managmentIp))
				{
					logger.info("the master is {}, not me, waiting...",
							new String(zk.getData("/s5/" + nodes[0], true, null)));
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
			zk.create("/s5/sstores", managmentIp.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
		catch (KeeperException | InterruptedException e)
		{
			logger.error("Interrupted or Zookeerper Exception:" + e);
		}
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
