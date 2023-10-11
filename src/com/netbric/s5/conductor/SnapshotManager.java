package com.netbric.s5.conductor;

import com.dieselpoint.norm.Transaction;
import com.netbric.s5.conductor.exception.InvalidParamException;
import com.netbric.s5.conductor.handler.VolumeHandler;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.RetCode;
import com.netbric.s5.conductor.rpc.SimpleHttpRpc;
import com.netbric.s5.orm.*;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;

public class SnapshotManager {
	static final Logger logger = LoggerFactory.getLogger(VolumeHandler.class);
	static Hashtable<Long, Volume> ongoingVolumes = new Hashtable<>();
	public static int createSnapshot(String tenant_name, String volume_name, String snap_name) throws InvalidParamException
	{
		Transaction t = null;
		Volume v ;
		Snapshot snap;

		v = Volume.fromName(tenant_name, volume_name);
		if(v == null)
			throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
		snap = Snapshot.fromName(tenant_name, volume_name, snap_name);
		if(snap != null)
			throw new InvalidParamException(String.format("Snapshot %s already exists", snap_name));

		t = S5Database.getInstance().startTransaction();
		v = S5Database.getInstance().sql("select * from t_volume where id=? for update", v.id).transaction(t).first(Volume.class);//lock db
		snap = new Snapshot();
		snap.name = snap_name;
		snap.size = v.size;
		snap.volume_id = v.id;
		snap.snap_seq = v.snap_seq;
		S5Database.getInstance().transaction(t).insert(snap);
		S5Database.getInstance().transaction(t).sql("update t_volume set snap_seq=snap_seq+1 where id=?", v.id)
				.execute();
		t.commit(); //commit change to snap_seq, even later push snap_seq tot store node failed, we waste a snap_seq number only,
		//this snap_seq number will never be reused.
		Volume v2 = Volume.fromId(v.id);//now get new meta_ver after snapshot
		boolean updateFailed = false;
		List<StoreNode> nodes = S5Database.getInstance()
				.sql("select * from t_store where id in (select distinct store_id from t_replica where volume_id=? and status=?)",
						v.id, Status.OK)
				.results(StoreNode.class);
		for(StoreNode n : nodes) {
			try {
				SimpleHttpRpc.invokeStore(n.mngtIp, "set_snap_seq", RestfulReply.class, "volume_id", v2.id, "snap_seq", v2.snap_seq);
			}
			catch(Exception e){
				updateFailed = true;
				VolumeHandler.updateReplicaStatusToError(n.id, v2.id, String.format("push snap_seq to store:%d fail", n.id));
				logger.error("Failed update snap_seq on store:{}({}), ", n.id, n.mngtIp);
				e.printStackTrace();
			}
		}
		if(updateFailed) {
			VolumeHandler.incrVolumeMetaver(v2, true, "create snapshot failed");
			return RetCode.REMOTE_ERROR;
		}
		return 0;
	}

	public static class ReplicaStore{
		public long id;
		public String mngt_ip;
		public String tray_uuid;
		public ReplicaStore(){}
	}
	public static int deleteSnapshot(String tenant_name, String volume_name, String snap_name) throws InvalidParamException {
		Volume v= Volume.fromName(tenant_name, volume_name);
		if(v == null)
			throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));

		if(ongoingVolumes.containsKey(v.id)){
			throw new InvalidParamException(String.format("Volume %s/%s has ongoing task", tenant_name, volume_name));
		}
		List<Snapshot> snaps = S5Database.getInstance().where("volume_id=?", v.id).orderBy("snap_seq").results(Snapshot.class);
		Snapshot prevSnap /* smaller sequence*/ = null, targetSnap /*targetSnap to delete*/=null, nextSnap /*bigger sequence*/=null;

		for(ListIterator<Snapshot> it = snaps.listIterator(); it.hasNext(); ){
			Snapshot s = it.next();
			if(s.name.equals(snap_name)){
				targetSnap = s;
				nextSnap = it.hasNext()? it.next() : null;
				break;
			} else {
				prevSnap = s;
			}
		}

		if(targetSnap == null){
			throw new InvalidParamException(String.format("Volume %s/%s has no snapshot:%s", tenant_name, volume_name, snap_name));
		}

		ongoingVolumes.put(v.id, v);
		S5Database.getInstance().delete(targetSnap); //delete it in db, regardless later object deleting fail.


		List<ReplicaStore> reps = S5Database.getInstance().sql("select r.id,  r.tray_uuid, s.mngt_ip from t_replica r, t_store s " +
				"where r.volume_id=? and s.status=? and r.store_id=s.id", v.id, Status.OK ).results(ReplicaStore.class);
		logger.debug("{} stores OK to delete snapshot", reps.size());
		for(ReplicaStore r : reps) {
			try {
				logger.debug("delete snapshot on store:{}", r.mngt_ip);
				SimpleHttpRpc.invokeStore(r.mngt_ip, "delete_snapshot", RestfulReply.class,
						"shard_id",VolumeIdUtils.replicaToShardId(r.id), "ssd_uuid", r.tray_uuid, "snap_seq", targetSnap.snap_seq,
						"prev_snap_seq", prevSnap == null ? 0:prevSnap.snap_seq, "next_snap_seq", nextSnap == null ? v.snap_seq:nextSnap.snap_seq);
			}
			catch(Exception e){
				logger.error("Failed delete snapshot on store:{}", r.mngt_ip);
				e.printStackTrace();
			}
		}
		ongoingVolumes.remove(v.id);
		return RetCode.OK;
	}
}

