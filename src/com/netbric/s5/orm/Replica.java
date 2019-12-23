package com.netbric.s5.orm;

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
	public String tray_uuid;
	public String status;

	public static final int STATUE_NORMAL = 0;
	public static final int STATUE_SLAVE = 1;
	public static final int STATUE_RECOVERING = 2;
	public static final int STATUE_ERROR = 3;

	public Replica()
	{
	};

}
