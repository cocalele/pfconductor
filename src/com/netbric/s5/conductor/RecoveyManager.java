package com.netbric.s5.conductor;

import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.SimpleHttpRpc;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Shard;
import com.netbric.s5.orm.Status;
import com.netbric.s5.orm.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.List;

public class RecoveyManager {
	static final Logger logger = LoggerFactory.getLogger(RecoveyManager.class);
	static RecoveyManager inst;
	public static RecoveyManager getInstance() {
		return inst;
	}
	public void recoveryVolume(BackgroundTaskManager.BackgroundTask task) throws Exception {
		Volume v = (Volume)task.arg;
		long total = S5Database.getInstance().queryLongValue("select count(*) from t_replica where volume_id=? and status != ?",  v.id, Status.OK);
		List<Shard> illhealthShards = S5Database.getInstance().where("volume_id=? and status != ?", v.id, Status.OK).results(Shard.class);
		for(Shard s : illhealthShards) {
			recoveryShard(task, total, s);
		}
		task.progress = 100;//100% completed
	}

	private void recoveryShard(BackgroundTaskManager.BackgroundTask task, long total, Shard s) throws Exception {
		class RepExt {
			public long id;
			public String store_ip;
			public int is_primary;
			public String status;
			public String ssd_uuid;
			public int object_size;
		}
		List<RepExt> replicas = S5Database.getInstance().where("shard_id=?", s.id).results(RepExt.class);
		RepExt primaryRep = null;
		Iterator<RepExt> it = replicas.iterator();
		while(it.hasNext()){
			RepExt r = it.next();

			if(r.is_primary == 1){
				primaryRep = r;
				it.remove();
				break;
			}
		}

		if(primaryRep == null || primaryRep.status != Status.OK) {
			throw new StateException(String.format("Shard:%x has no primary replica available", s.id));
		}

		for(RepExt r : replicas) {
			class MetaV {
				public int meta_ver;
			}
			logger.info("Begin recovery replica: {} on store:{} from primary:{}", String.format("0x%x",r.id), r.store_ip, primaryRep.store_ip);
			MetaV meta = S5Database.getInstance().sql("select meta_ver from t_volume where id=?", VolumeIdUtils.replicaToVolumeId(r.id)).first(MetaV.class);
			SimpleHttpRpc.invokeStore(primaryRep.store_ip, "begin_recovery", RestfulReply.class, "replica_id", r.id);
			//SimpleHttpRpc.invokeStore(r.store_ip, "begin_recovery", RestfulReply.class, "replica_id", r.id);
			SimpleHttpRpc.invokeStore(r.store_ip, "recovery_replica", RestfulReply.class, "from", primaryRep.store_ip);
			//SimpleHttpRpc.invokeStore(r.store_ip, "end_recovery", RestfulReply.class, "replica_id", r.id);
			SimpleHttpRpc.invokeStore(primaryRep.store_ip, "end_recovery", RestfulReply.class, "replica_id", r.id);
			MetaV metaOnEnd = S5Database.getInstance().sql("select meta_ver from t_volume where id=?", VolumeIdUtils.replicaToVolumeId(r.id)).first(MetaV.class);
			if(meta.meta_ver != metaOnEnd.meta_ver) {
				logger.error("Meta version has changed during recovery, give up");
				throw new StateException("Meta version has changed during recovery, give up");
			}
			logger.info("Succeeded recovery replica: {} on store:{} from primary:{}", String.format("0x%x",r.id), r.store_ip, primaryRep.store_ip);
			task.progress += 100/total;
		}
	}
}
