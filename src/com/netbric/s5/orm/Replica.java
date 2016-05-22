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
	@GeneratedValue
	@Column(name = "idx")
	public int idx;
	public int volume_idx;
	public int store_idx;
	public int tray_id;
	public int status;

	public static final int STATUE_NORMAL = 0;
	public static final int STATUE_SLAVE = 1;
	public static final int STATUE_RECOVERING = 2;
	public static final int STATUE_ERROR = 3;

	public Replica()
	{
	};

	public Replica(int idx, int volume_idx, int tray_id, int status, int store_idx)
	{
		super();
		this.volume_idx = volume_idx;
		this.store_idx = store_idx;
		this.tray_id = tray_id;
		this.status = status;
	}

	@Transient
	public int getReplicaId()
	{
		return volume_idx << 8 | tray_id;
	}
}
