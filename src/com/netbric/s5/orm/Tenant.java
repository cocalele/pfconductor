package com.netbric.s5.orm;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_tenant")
public class Tenant
{
	@Id
	public int id;
	public int car_id;
	public String name;// varchar(96),
	public String pass_wd;
	public int auth;
	public long size;
	public int iops;
	public int cbs;
	public int bw;

	public Tenant()
	{
	};

	public Tenant(int idx, int car_id, String name, String passwd, int auth, int size, int iops, int cbs, int bw)
	{
		super();
		this.id = idx;
		this.car_id = car_id;
		this.name = name;
		this.pass_wd = passwd;
		this.auth = auth;
		this.size = size;
		this.iops = iops;
		this.cbs = cbs;
		this.bw = bw;
	}
}
