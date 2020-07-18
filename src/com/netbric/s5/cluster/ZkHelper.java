package com.netbric.s5.cluster;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeCreated;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

public class ZkHelper {
	static final Logger logger = LoggerFactory.getLogger(ZkHelper.class);
	public ZooKeeper zk;
	public ZkHelper(ZooKeeper zk) {
		this.zk = zk;
	}

	public void createZkNodeIfNotExist(String path, String data) throws KeeperException, InterruptedException {
		if(zk.exists(path, false) != null)
			return;
		zk.create(path, data == null ? null : data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

	}

	public static abstract class NewChildCallback {
		abstract void onNewChild(String childPath);
	}
	public static abstract class NodeChangeCallback {
		abstract void onNodeCreate(String childPath);
		abstract void onNodeDelete(String childPath);
	}
	class ZkWatcher implements Watcher {
		Semaphore s;
		ZkWatcher(Semaphore s) {
			this.s = s;
		}
		@Override
		public void process(WatchedEvent event) {
			switch (event.getType()) {
				case NodeCreated:
					logger.info("NodeCreated, path:{}", event.getPath());
					break;
				case NodeDeleted:
					logger.info("NodeDeleted, path:{}", event.getPath());
					break;
				case NodeChildrenChanged:
					logger.info("NodeChildrenChanged, path:{}", event.getPath());
					s.release();
					break;
			}
		}
	}
	public void watchNewChild(String parentPath, NewChildCallback cbk){
		new Thread(()->{
			try {
				Semaphore s = new Semaphore(0);
				List<String> children2 = zk.getChildren(parentPath, new ZkWatcher(s));
				while (true) {
					s.acquire();
					List<String> children = zk.getChildren(parentPath, new ZkWatcher(s));
					HashSet<String> origin = new HashSet<>(children2);
					for (String node : children) {
						if (!origin.contains(node)) {
							String newNode = parentPath + "/" + node;
							logger.info("New node found from zk:{}", newNode);
							cbk.onNewChild(newNode);
						}
					}
					children2 = children;
				}
			} catch (Exception e) {
				logger.error("Exception in watching {}, {}", parentPath, e.toString());
				System.exit(1);
			}
		}).start();
	}


	public  void watchNode(String nodePath, NodeChangeCallback cbk) {
		new Thread(()->{
			try {
				Semaphore s = new Semaphore(0);
				while(true) {
					zk.exists(nodePath, event -> {
						s.release();
						logger.info("ZK event:{} on path:{}", event.getType().toString(), event.getPath());
						if(event.getType() == NodeCreated)
							cbk.onNodeCreate(event.getPath());
						else if(event.getType() == NodeDeleted)
							cbk.onNodeDelete(event.getPath());
						else {
							logger.error("Unexpected zk event:{}", event.getType().toString());
							return;
						}
					});
					s.acquire();
				}
			}
			catch (Exception e)
			{
				logger.error("Error during watch alive:", e);
			}
		}).start();

	}

}
