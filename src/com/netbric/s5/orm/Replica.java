package com.netbric.s5.orm;

import com.netbric.s5.conductor.VolumeIdUtils;

import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.sql.Timestamp;

@Table(name = "t_replica")
public class Replica
{
	@Id
	public long id;
	public long volume_id;
	public long shard_id;
	public int store_id;
	public int replica_index;
	public String tray_uuid;
	public String status;
	public Timestamp status_time;
	public Replica()
	{
	};

	@javax.persistence.Transient
	public int getShardIndex()
	{
		return VolumeIdUtils.replicaToShardIndex(id);
	}

	@javax.persistence.Transient
	@Override
	public Replica clone()
	{
		Replica r = new Replica();
		r.id = id;
		r.volume_id = volume_id;
		r.shard_id = shard_id;
		r.store_id = store_id;
		r.replica_index = replica_index;
		r.tray_uuid = tray_uuid;
		r.status = status;
		r.status_time = status_time;
		return r;
	}
}
