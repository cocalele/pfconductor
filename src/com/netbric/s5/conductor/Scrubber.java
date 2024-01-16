package com.netbric.s5.conductor;

import com.dieselpoint.norm.DbException;
import com.dieselpoint.norm.Transaction;
import com.netbric.s5.conductor.exception.InvalidParamException;
import com.netbric.s5.conductor.handler.VolumeHandler;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.SimpleHttpRpc;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Shard;
import com.netbric.s5.orm.Status;
import com.netbric.s5.orm.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Transient;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

public class Scrubber {
	static final Logger logger = LoggerFactory.getLogger(Scrubber.class);

	public static final class RepExtTemp{
		public RepExtTemp(){}
		public long replica_index;
		public long replica_id;
		public String tray_uuid;
		public String mngt_ip;
		@Transient
		public String  md5;
		@Transient
		public ObjectMd5Reply obj_md5_reply;
		public long object_size;
	};
	public static final class CalcMd5Reply  extends RestfulReply
	{
		public String md5;
		public CalcMd5Reply(String op) {
			super(op);
		}
	}


	public static final class ObjectMd5Reply  extends RestfulReply
	{
		public static final class SingleMd5{
			public long snap_seq;
			public String md5;
			@Override
			public boolean equals(Object other){
				if(! (other instanceof SingleMd5 ))
					return false;
				SingleMd5 o = (SingleMd5)other;
				return snap_seq== o.snap_seq && md5.equals(o.md5);
			}
		}
		public long rep_id;
		public long offset_in_vol;
		public Vector<SingleMd5> snap_md5 = new Vector<>();

		public ObjectMd5Reply(String op) {
			super(op);
		}
		public boolean compare(ObjectMd5Reply other)
		{
			if(rep_id != other.rep_id || offset_in_vol != other.offset_in_vol)
				return false;
			if(snap_md5.size() != other.snap_md5.size())
			{
				logger.error("Snapshot count not equal, rep_id:{} off:{}", rep_id, offset_in_vol);
				return false;
			}
			for(int i=0;i<snap_md5.size();i++){
				if(!snap_md5.get(i).equals(other.snap_md5.get(i))) {
					logger.error("Object md5 mismatch, rep_id:{} off:{} snap_seq:%d", rep_id, offset_in_vol, snap_md5.get(i).snap_seq);
					return false;
				}
			}
			return true;
		}
	}
	public static BackgroundTaskManager.BackgroundTask scrubVolume(String tenant_name, String volume_name) throws InvalidParamException {
		Volume v = Volume.fromName(tenant_name, volume_name);
		if(v == null)
			throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
		BackgroundTaskManager.BackgroundTask t=null;

		t = BackgroundTaskManager.getInstance().initiateTask(
				BackgroundTaskManager.TaskType.SCRUB, "scrub volume:" + volume_name,
				new BackgroundTaskManager.TaskExecutor() {
					public void run(BackgroundTaskManager.BackgroundTask t) throws Exception {
						List<Shard> shards = S5Database.getInstance().where("volume_id=?", v.id).results(Shard.class);
						int progress = 1;
						int inconsist_cnt = 0;
						for(Shard s : shards) {
							List<RepExtTemp> reps = S5Database.getInstance().
									sql("select r.id replica_id, r.tray_uuid, replica_index, mngt_ip from t_replica r, t_store s " +
											" where r.store_id=s.id and r.shard_id=? and r.status=?", s.id, Status.OK).results(RepExtTemp.class);
							RepExtTemp primary = null;
							for(RepExtTemp r : reps) {
								CalcMd5Reply reply = SimpleHttpRpc.invokeStore(r.mngt_ip, "calculate_replica_md5", CalcMd5Reply.class,
										"replica_id", r.replica_id,
										"ssd_uuid", r.tray_uuid);
								if(reply.retCode != 0) {
									logger.error("Failed to get replica md5 from store:{} for: {}", r.mngt_ip, reply.reason);
								} else {
									logger.info("Get replica:0x{} md5:{}", Long.toHexString(r.replica_id), reply.md5);
								}
								if(r.replica_index == s.primary_rep_index)
									primary =r;
								r.md5 = reply.md5;

							}
							if(primary == null || primary.md5 == null ){
								logger.error("Primary replica not available for shard:0x{}", Long.toHexString(s.id));
								continue;
							}
							for(RepExtTemp r : reps) {
								if(!primary.md5.equals(r.md5)) {
									inconsist_cnt++;
									logger.error("Replica 0x:{} md5:{} diff to primary:{}", Long.toHexString(r.replica_id), r.md5, primary.md5);
									logger.error("SET_REPLICA_STATUS_ERROR_ Set replica:0x{} to ERROR status, for scrub", Long.toHexString(r.replica_id));
									Transaction trans = S5Database.getInstance().startTransaction();
									try {
										S5Database.getInstance().transaction(trans)
												.sql("update t_replica set status='ERROR' where id=?", r.replica_id).execute();
										S5Database.getInstance().transaction(trans).sql("update t_shard set status=? where id=?", Status.DEGRADED, s.id).execute();
										int changed = S5Database.getInstance().transaction(trans).sql(
														"update t_volume set status=? where id=?", Status.DEGRADED, v.id)
												.execute().getRowsAffected();
										if(changed > 0) {
											logger.warn("Volume:{} status changed due to scrub", v.id);
										}
										trans.commit();;
									}catch(DbException e) {
										trans.rollback();
										e.printStackTrace();
										logger.error("Failed to update DB and will suicide ... \n", e);
										Main.suicide();
									}
									finally{
										if(trans != null){
											try {
												trans.close();
											} catch (IOException e) {
												logger.error("Failed close transaction", e);
											}
										}

									}
									VolumeHandler.pushMetaverToStore(v);
								}

							}

							t.progress = progress*100/shards.size();
							progress++;
						}
						logger.info("scrub volume:{}, id:0x{} complete, {} replicas are inconsistency", v.name,
								Long.toHexString(v.id), inconsist_cnt);
						t.status = (inconsist_cnt == 0 ? BackgroundTaskManager.TaskStatus.SUCCEEDED : BackgroundTaskManager.TaskStatus.FAILED);
					}
				}, v);
		return t;
	}

	public static BackgroundTaskManager.BackgroundTask deepScrubVolume(String tenant_name, String volume_name) throws InvalidParamException {
		Volume v = Volume.fromName(tenant_name, volume_name);
		if(v == null)
			throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
		BackgroundTaskManager.BackgroundTask t=null;

		t = BackgroundTaskManager.getInstance().initiateTask(
				BackgroundTaskManager.TaskType.DEEP_SCRUB, "scrub volume:" + volume_name,
				new BackgroundTaskManager.TaskExecutor() {
					public void run(BackgroundTaskManager.BackgroundTask t) throws Exception {
						List<Shard> shards = S5Database.getInstance().where("volume_id=?", v.id).results(Shard.class);
						int progress = 1;
						int inconsist_cnt = 0;
						for(Shard s : shards) {
							List<RepExtTemp> reps = S5Database.getInstance().
									sql("select r.id replica_id, r.tray_uuid, replica_index, mngt_ip, t.object_size " +
											" from t_replica r, t_store s, t_tray t " +
											" where r.store_id=s.id and r.shard_id=? and r.status=? and t.uuid=r.tray_uuid", s.id, Status.OK).results(RepExtTemp.class);
							RepExtTemp primary = null;
							long object_size = reps.get(0).object_size;
							for(int obj_idx = 0; obj_idx < VolumeIdUtils.SHARD_SIZE/object_size; obj_idx ++) {

								for (RepExtTemp r : reps) {
									if(r.object_size != object_size){
										logger.error("Failed scrub, replicas has different object size, can't continue");
										t.status = BackgroundTaskManager.TaskStatus.FAILED;
										return;
									}
									ObjectMd5Reply reply = SimpleHttpRpc.invokeStore(r.mngt_ip, "calculate_object_md5", ObjectMd5Reply.class,
											"replica_id", r.replica_id, "object_index", obj_idx,
											"ssd_uuid", r.tray_uuid);
									if (reply.retCode != 0) {
										logger.error("Failed to get object md5 from store:{} for: {}", r.mngt_ip, reply.reason);
									} else {

									}
									if (r.replica_index == s.primary_rep_index)
										primary = r;
									r.obj_md5_reply = reply;

								}
								if (primary == null) {
									logger.error("Primary replica not available for shard:0x{}", Long.toHexString(s.id));
									primary = reps.get(0);
								}
								for (RepExtTemp r : reps) {
									if (!primary.obj_md5_reply.compare(r.obj_md5_reply)) {
										inconsist_cnt++;
										logger.error("Replica 0x:{} obj_index:{} diff to primary", Long.toHexString(r.replica_id), obj_idx);
										logger.error("SET_REPLICA_STATUS_ERROR_ Set replica:0x{} to ERROR status, for scrub", Long.toHexString(r.replica_id));
										Transaction trans = S5Database.getInstance().startTransaction();
										try {
											S5Database.getInstance().transaction(trans)
													.sql("update t_replica set status='ERROR' where id=?", r.replica_id).execute();
											S5Database.getInstance().transaction(trans).sql("update t_shard set status=? where id=?", Status.DEGRADED, s.id).execute();
											int changed = S5Database.getInstance().transaction(trans).sql(
															"update t_volume set status=? where id=?", Status.DEGRADED, v.id)
													.execute().getRowsAffected();
											if (changed > 0) {
												logger.warn("Volume:{} status changed due to scrub", v.id);
											}
											trans.commit();
											;
										} catch (DbException e) {
											trans.rollback();
											e.printStackTrace();
											logger.error("Failed to update DB and will suicide ... \n", e);
											Main.suicide();
										}
										finally{
											if(trans != null){
												try {
													trans.close();
												} catch (IOException e) {
													logger.error("Failed close transaction", e);
												}
											}

										}
										VolumeHandler.pushMetaverToStore(v);
									}

								}
							}
							t.progress = progress*100/shards.size();
							progress++;
						}
						logger.info("scrub volume:{}, id:0x{} complete, {} replicas are inconsistency", v.name,
								Long.toHexString(v.id), inconsist_cnt);
						t.status = (inconsist_cnt == 0 ? BackgroundTaskManager.TaskStatus.SUCCEEDED : BackgroundTaskManager.TaskStatus.FAILED);
					}
				}, v);
		return t;
	}
}
