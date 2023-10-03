package com.netbric.s5.conductor;

import com.dieselpoint.norm.Transaction;
import com.netbric.s5.conductor.exception.LoggedException;
import com.netbric.s5.conductor.handler.VolumeHandler;
import com.netbric.s5.conductor.rpc.BackgroundTaskReply;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.SimpleHttpRpc;
import com.netbric.s5.conductor.rpc.StoreQueryTaskReply;
import com.netbric.s5.orm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RebalanceManager {
	static final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);
	static RebalanceManager inst = new RebalanceManager();
	public static RebalanceManager getInstance() {
		return inst;
	}




	public void moveVolume(BackgroundTaskManager.BackgroundTask task, long fromStoreId, String fromSsdUuid,
	                       long targetStoreId, String targetSsdUuid) throws Exception {
		Volume v = (Volume)task.arg;
		List<Replica> repsToMove = S5Database.getInstance().sql("select * from t_replica where volume_id=? and store_id=? and tray_uuid=?",
				v.id, fromStoreId, fromSsdUuid).results(Replica.class);
		long total = repsToMove.size();
		int i=0;
		StoreNode n = StoreNode.fromId(targetStoreId);
		for(Replica r : repsToMove) {
			moveReplica(v, r, n, targetSsdUuid);
			i++;
			task.progress = (int)(i*100/total);
		}
		S5Database.getInstance().sql("update t_volume set status=IF((select count(*) from t_shard where status!='OK' and volume_id=?) = 0, 'OK', status)" +
				" where id=?", v.id, v.id).execute();
		task.progress = 100;//100% completed
	}

	private void moveReplica(Volume vol, Replica r, StoreNode targetStore, String targetSsdUuid) throws Exception
	{
		if(!r.status.equals("OK"))
		{
			throw new LoggedException(logger, "Replica:0x%s status:%s can't move. Generally replica in error can recovery directly and not need to move",
					Long.toHexString(r.id),  r.status);

		}

		logger.info("Begin move replica: {} from store:{} to store:{} ssd:{}", String.format("0x%x",r.id), r.store_id,targetStore.id, targetSsdUuid);
		long meta = S5Database.getInstance().queryLongValue("select meta_ver from t_volume where id=?", VolumeIdUtils.replicaToVolumeId(r.id));

		long cnt = S5Database.getInstance().queryLongValue("select count(*) from t_replica where volume_id=? and store_id=?", vol.id, targetStore.id);
		boolean openedOnTarget = cnt > 0;
		VolumeHandler.PrepareVolumeArg arg = VolumeHandler.getPrepareArgs(vol, null);
		VolumeHandler.ReplicaArg origRep = arg.shards.get(r.getShardIndex()).replicas.get(r.replica_index);
		VolumeHandler.ShardArg shard = arg.shards.get(r.getShardIndex());
		VolumeHandler.ReplicaArg targetRep = origRep.clone();
		targetRep.tray_uuid = targetSsdUuid;
		targetRep.store_id = targetStore.id;
		targetRep.status = Status.OK;

		shard.replicas.set(r.replica_index, targetRep);
		if(!openedOnTarget){
			VolumeHandler.prepareVolumeOnStore(targetStore, arg);

		} else {
			arg.shards.clear();
			arg.shards.add(shard);
			VolumeHandler.prepareShardsOnStore(targetStore, arg);
		}

		shard.replicas.set(r.replica_index, origRep); //restore to origin replica
		targetRep.index = shard.replicas.size();
		shard.replicas.add(targetRep); //add the extra replica to original master

		StoreNode master = StoreNode.fromId(shard.replicas.get((int)shard.primary_rep_index).store_id);
		arg.shards.clear();
		arg.shards.add(shard);
		VolumeHandler.prepareShardsOnStore(master, arg);

		RestfulReply reply = SimpleHttpRpc.invokeStore(targetStore.mngtIp, "begin_recovery", RestfulReply.class, "replica_id", r.id);
		if(reply.retCode != 0) {
			throw new LoggedException(logger, "move replica failed, begin_recovery fail on:{} failed, reason:{}", targetStore.mngtIp, reply.reason);
		}

		reply = SimpleHttpRpc.invokeStore(master.mngtIp, "begin_recovery", RestfulReply.class,
				"replica_id", r.id, "replica_index", shard.replicas.size()-1);
		if(reply.retCode != 0) {
			throw new LoggedException(logger, "move replica failed, begin_recovery fail on:{}, reason:{}", master.mngtIp, reply.reason);
		}


		RecoveryManager.RepExt srcRepInfo = S5Database.getInstance().sql(
				" select replica_id,mngt_ip store_ip, is_primary, r.status , s.id store_id, r.tray_uuid ssd_uuid,  t.object_size, s.status store_status " +
						"from v_replica_ext r, t_store s , t_tray t " +
						"where r.store_id=s.id and t.uuid=r.tray_uuid and is_primary=1 and r.shard_id=?", VolumeIdUtils.replicaToShardId(r.id)).first(RecoveryManager.RepExt.class);


		VolumeHandler.ReplicaArg masterRep = shard.replicas.get((int)shard.primary_rep_index);
		BackgroundTaskReply store_task = SimpleHttpRpc.invokeStore(targetStore.mngtIp, "recovery_replica", BackgroundTaskReply.class,
				"replica_id", Long.toString(r.id), "meta_ver", meta,
				"from_store_id", Long.toString(master.id), "from_store_mngt_ip", master.mngtIp, "from_ssd_uuid", masterRep.tray_uuid,
				"ssd_uuid", targetRep.tray_uuid,	"object_size", Long.toString(srcRepInfo.object_size));
		logger.info("moving replica to store:{} task_id:{} ...", targetStore.mngtIp, store_task.taskId);
		int recovery_ok = 0;
		while(true) {
			Thread.sleep(10000);
			StoreQueryTaskReply status = SimpleHttpRpc.invokeStore(targetStore.mngtIp, "query_task", StoreQueryTaskReply.class, "task_id", store_task.taskId);
			if(status.retCode != 0){
				logger.error("query task failed on: {} failed, reason:{} ", master.mngtIp, status.reason);
				logger.error("move replica:0x{} to store:{}", Long.toHexString(r.id), targetStore.mngtIp);
				recovery_ok = -1;
				break;
			}
			if(status.status == BackgroundTaskManager.TaskStatus.SUCCEEDED) {
				logger.info("Succeeded sync extra replica: {} on store:{} ", String.format("0x%x",r.id), targetStore.mngtIp);
				recovery_ok = 1;
				break;
			} else if(status.status == BackgroundTaskManager.TaskStatus.FAILED){
				logger.error("Failed sync extra replica:0x{} on store:{}, reason:{}", String.format("0x%x",r.id), targetStore.mngtIp, status.reason);
				recovery_ok = 0;
				break;
			}
		}
		logger.info("Finish move replica data: {} from store:{} to store:{} ssd:{}, status:%d",
				String.format("0x%x",r.id), r.store_id,targetStore.id, targetSsdUuid, recovery_ok);

		if(recovery_ok >= 0) {
			reply = SimpleHttpRpc.invokeStore(master.mngtIp, "end_recovery", RestfulReply.class,
					"replica_id", r.id,
					"replica_index", shard.replicas.size()-1, "ok", recovery_ok);
			if(reply.retCode != 0) {
				throw new LoggedException(logger, "end_recovery on primary node:%s failed, reason:%s", master.mngtIp, reply.reason);
			}
			//though  end_recovery, there are still 3+1=4 replicas on primary node.
			reply = SimpleHttpRpc.invokeStore(targetStore.mngtIp, "end_recovery", RestfulReply.class, "replica_id", r.id, "ok", recovery_ok);
			if(reply.retCode != 0) {
				throw new LoggedException(logger, "end_recovery on slave node:{} failed, reason:{}", targetStore.mngtIp, reply.reason);
			}

			Transaction tx = S5Database.getInstance().startTransaction();
			try{
				S5Database.getInstance().transaction(tx).sql("select * from t_volume where id=? for update", vol.id); //ensure no other can update meta
				long metaOnEnd = S5Database.getInstance().queryLongValue(tx, "select meta_ver from t_volume where id=?", VolumeIdUtils.replicaToVolumeId(r.id));
				if (meta != metaOnEnd) {
					throw new LoggedException(logger, "Meta version has changed from:%d to %d, during move replica:0x%x,  give up!", meta, metaOnEnd, r.id);
				}
				S5Database.getInstance().transaction(tx).sql("update t_replica set status='OK', store_id=?, tray_uuid=? where id=? ",
						targetStore.id, targetSsdUuid, r.id).execute();
				S5Database.getInstance().sql("update t_volume set meta_ver=meta_ver+1 where id=?", vol.id).execute();
				Volume v2 = Volume.fromId(vol.id);
				logger.warn("increase volume:{} metaver from {} to {}, for:{}",vol.name, vol.meta_ver, v2.meta_ver, "move replica");
				tx.commit();
				arg.meta_ver = v2.meta_ver;
				shard.replicas.remove(targetRep.index);
				targetRep.index = r.replica_index;
				shard.replicas.set(r.replica_index, targetRep);
				VolumeHandler.prepareShardsOnStore(master, arg);
				VolumeHandler.pushMetaverToStore(vol);
				logger.info("Succeeded  move replica: {} from store:{} to store:{} ssd:{}", String.format("0x%x", r.id), r.store_id, targetStore.id, targetSsdUuid);
			} catch(Exception e) {
				tx.rollback();
				logger.error("Failed  move replica: {} from store:{} to store:{} ssd:{}. reason:{}", String.format("0x%x", r.id),
						r.store_id, targetStore.id, targetSsdUuid, e);
			}
		}
	}
}
