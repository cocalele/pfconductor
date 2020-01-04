package com.netbric.s5.orm;

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
	public int store_id;
	public String tray_uuid;
	public String status;
	public Timestamp status_time;
	public Replica()
	{
	};

}
