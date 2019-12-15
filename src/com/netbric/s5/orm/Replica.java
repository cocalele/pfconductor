package com.netbric.s5.orm;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

@Table(name = "t_replica")
public class Replica
{
	@Id
	public long id;
	public long volume_id;
	public int store_id;
	public int tray_id;
	public int status;

	public static final int STATUE_NORMAL = 0;
	public static final int STATUE_SLAVE = 1;
	public static final int STATUE_RECOVERING = 2;
	public static final int STATUE_ERROR = 3;

	public Replica()
	{
	};

	public Replica(long idx, long volume_id, int tray_id, int status, int store_id)
	{
		super();
		this.volume_id = volume_id;
		this.store_id = store_id;
		this.tray_id = tray_id;
		this.status = status;
	}

	@Transient
	public long getReplicaId()
	{
		return volume_id << 8 | tray_id;
	}
}
