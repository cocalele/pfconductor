package com.netbric.s5.orm;

import javax.persistence.*;

@Table(name = "t_volume")
public class Volume
{
	@Id
	public long id;
	public String name;// varchar(96),
	public long size;
	public int iops;
	public int cbs;
	public int bw;
	@OneToOne
	@JoinColumn(name = "id")
	@JoinTable(name="t_tenant")
	public int tenant_id;
	public int quotaset_id;
	public String status;
	public boolean exposed;
	public int primary_rep_id;
	public int rep_count; //replica count, 1, 2, 3


	public Volume()
	{
	};


}
