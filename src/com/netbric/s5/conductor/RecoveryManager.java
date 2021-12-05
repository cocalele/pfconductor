package com.netbric.s5.conductor;

import com.netbric.s5.conductor.handler.VolumeHandler;
import com.netbric.s5.conductor.rpc.BackgroundTaskReply;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.SimpleHttpRpc;
import com.netbric.s5.conductor.rpc.StoreQueryTaskReply;
import com.netbric.s5.orm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.List;

public class RecoveryManager {
	static final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);
	static RecoveryManager inst = new RecoveryManager();
	public static RecoveryManager getInstance() {
		return inst;
	}
	public void recoveryVolume(BackgroundTaskManager.BackgroundTask task) throws Exception {
		Volume v = (Volume)task.arg;
		long total = S5Database.getInstance().queryLongValue("select count(*) from t_replica where volume_id=? and status != ?",  v.id, Status.OK);
		List<Shard> illhealthShards = S5Database.getInstance().where("volume_id=? and status != ?", v.id, Status.OK).results(Shard.class);
		for(Shard s : illhealthShards) {
			recoveryShard(task, v, total, s);
		}
		S5Database.getInstance().sql("update t_volume set status=IF((select count(*) from t_shard where status!='OK' and volume_id=?) = 0, 'OK', status)" +
				" where id=?", v.id, v.id).execute();
		task.progress = 100;//100% completed
	}
	public static class RepExt {
		public long replica_id;
		public String store_ip;
		public long is_primary;
		public String status;
		public long store_id;
		public String ssd_uuid;
		public long object_size;
		public String store_status;

	}
	private void recoveryShard(BackgroundTaskManager.BackgroundTask task, Volume vol,  long total, Shard s) throws Exception {

		List<RepExt> replicas = S5Database.getInstance().sql(
				" select replica_id,mngt_ip store_ip, is_primary, r.status , s.id store_id, r.tray_uuid ssd_uuid,  t.object_size, s.status store_status " +
						"from v_replica_ext r, t_store s , t_tray t " +
						"where r.store_id=s.id and t.uuid=r.tray_uuid and r.shard_id=?", s.id).results(RepExt.class);
		RepExt primaryRep = null;
		Iterator<RepExt> it = replicas.iterator();
		while(it.hasNext()){
			RepExt r = it.next();

			if(r.is_primary == 1){
//				logger.info("Find primary replica:0x{}, status:{}", Long.toHexString(r.replica_id), r.status);
				primaryRep = r;
				it.remove();
				break;
			}
		}

		if(primaryRep == null || !primaryRep.status.equals(Status.OK)) {
			throw new StateException(String.format("Shard:%x has no primary replica available", s.id));
		}

		for(RepExt r : replicas) {
			if(r.status.equals("OK"))
				continue;
			if(!r.store_status.equals("OK")){
				logger.error("Replica:0x{} on store:[{}, status {}] not recoverable ", Long.toHexString(r.replica_id), r.store_ip, r.store_status);
				continue;
			}
			logger.info("Begin recovery replica: {} on store:{} from primary:{}", String.format("0x%x",r.replica_id), r.store_ip, primaryRep.store_ip);
			long meta = S5Database.getInstance().queryLongValue("select meta_ver from t_volume where id=?", VolumeIdUtils.replicaToVolumeId(r.replica_id));
			RestfulReply reply = SimpleHttpRpc.invokeStore(r.store_ip, "begin_recovery", RestfulReply.class, "replica_id", r.replica_id);
			if(reply.retCode != 0) {
				logger.error("begin_recovery on slave node:{} failed, reason:{}", r.store_ip, reply.reason);
				throw new Exception(reply.reason);
			}
			reply = SimpleHttpRpc.invokeStore(primaryRep.store_ip, "begin_recovery", RestfulReply.class, "replica_id", r.replica_id);
			if(reply.retCode != 0) {
				logger.error("begin_recovery on primary node:{} failed, reason:{}", primaryRep.store_ip, reply.reason);
				throw new Exception(reply.reason);
			}
			BackgroundTaskReply store_task = SimpleHttpRpc.invokeStore(r.store_ip, "recovery_replica", BackgroundTaskReply.class,
					"replica_id", Long.toString(r.replica_id), "meta_ver", vol.meta_ver,
					"from_store_id", Long.toString(primaryRep.store_id), "from_store_mngt_ip", primaryRep.store_ip, "from_ssd_uuid", primaryRep.ssd_uuid,
					"ssd_uuid", r.ssd_uuid,	"object_size", Long.toString(primaryRep.object_size));
			logger.info("recovery replica on store:{} task_id:{}", r.store_ip, store_task.taskId);
			int recovery_ok = 0;
			while(true) {
				Thread.sleep(10000);
				StoreQueryTaskReply status = SimpleHttpRpc.invokeStore(r.store_ip, "query_task", StoreQueryTaskReply.class, "task_id", store_task.taskId);
				if(status.retCode != 0){
					logger.error("query task failed on: {} failed, reason:{} ", r.store_ip, status.reason);
					logger.error("Failed recovery replica:0x{} on store:{}", Long.toHexString(r.replica_id), r.store_ip);
					recovery_ok = -1;
					break;
				}
				if(status.status == BackgroundTaskManager.TaskStatus.SUCCEEDED) {
					logger.info("Succeeded recovery replica: {} on store:{} from primary:{}", String.format("0x%x",r.replica_id), r.store_ip, primaryRep.store_ip);
					recovery_ok = 1;
					break;
				} else if(status.status == BackgroundTaskManager.TaskStatus.FAILED){
					logger.error("Failed recovery replica:0x{} on store:{} complete failed", Long.toHexString(r.replica_id), r.store_ip);
					recovery_ok = 0;
					break;
				}
			}
			if(recovery_ok >= 0) {
				long metaOnEnd = S5Database.getInstance().queryLongValue("select meta_ver from t_volume where id=?", VolumeIdUtils.replicaToVolumeId(r.replica_id));
				if(meta != metaOnEnd) {
					logger.error("Meta version has changed during recovery from:{} to {}, give up", meta, metaOnEnd);
					throw new StateException("Meta version has changed during recovery, give up");
				}
				reply = SimpleHttpRpc.invokeStore(primaryRep.store_ip, "end_recovery", RestfulReply.class, "replica_id", r.replica_id, "ok", recovery_ok);
				if(reply.retCode != 0) {
					logger.error("end_recovery on primary node:{} failed, reason:{}", primaryRep.store_ip, reply.reason);
					throw new Exception(reply.reason);
				}
				reply = SimpleHttpRpc.invokeStore(r.store_ip, "end_recovery", RestfulReply.class, "replica_id", r.replica_id, "ok", recovery_ok);
				if(reply.retCode != 0) {
					logger.error("end_recovery on slave node:{} failed, reason:{}", r.store_ip, reply.reason);
					throw new Exception(reply.reason);
				}
				logger.info("SET_REPLICA_STATUS_OK Set replica:0x{} to OK status, recovery succeeded", Long.toHexString(r.replica_id));
				S5Database.getInstance().sql("update t_replica set status='OK' where id=?", r.replica_id).execute();
			}

			task.progress += 100/total;
		}

		//meta_ver has already increased by MySQL trigger
		VolumeHandler.pushMetaverToStore(vol);
		S5Database.getInstance().sql("update t_shard set status=IF((select count(*) from t_replica  where status='ERROR' and shard_id=?) = 0, 'OK', status)" +
				" where id=?", s.id, s.id).execute();
	}

}
