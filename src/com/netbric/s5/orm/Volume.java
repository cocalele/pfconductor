package com.netbric.s5.orm;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Table(name = "t_volume")
public class Volume
{
	@Id
	@GeneratedValue
	public int idx;
	public int car_id;
	public String name;// varchar(96),
	public int access;
	public long size;
	public int iops;
	public int cbs;
	public int bw;
	@OneToOne
	@JoinColumn(name = "idx")
	public int tenant_idx;
	@OneToOne
	@JoinColumn(name = "idx")
	public int quotaset_idx;
	public int flag;
	public int status; // values can be one of STATUS_XXX
	public int exposed;
	public int primary_rep_idx;

	// following values are copied from C definition to enum s5c_volume_status
	public final static int STATUS_OK = 1; /// < 0001, status OK
	public final static int STATUS_CREATING = 2; /// < 0010, volume in creating
													/// status
	public final static int STATUS_DELETING = 4; /// < 0100, volume in deleting
													/// status
	public final static int STATUS_ERROR = 8; /// < 1000, volume in error status
	public final static int STATUS_RESERVE = 16; /// < 10000, reserved volume

	public Volume()
	{
	};

	public Volume(int car_id, String name, int access, int size, int iops, int cbs, int bw, int tenant_idx,
			int quotaset_idx, int flag, int status)
	{
		super();
		this.car_id = car_id;
		this.name = name;
		this.access = access;
		this.size = size;
		this.iops = iops;
		this.cbs = cbs;
		this.bw = bw;
		this.tenant_idx = tenant_idx;
		this.quotaset_idx = quotaset_idx;
		this.flag = flag;
		this.status = status;
	}
}
