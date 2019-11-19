package com.netbric.s5.orm;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_s5store")
public class StoreNode
{
	@Id
	@GeneratedValue
	public int idx;
	public String name;
	public String sn;
	public String model;
	public int status; // value can be one of S5C_STORE_XXX,
	@Column(name = "mngt_ip")
	public String mngtIp;

	// following values are copied from C definition to s5c_store_status
	public static final int STATUS_NULL = -1;
	public static final int STATUS_USABLE = 0;
	public static final int STATUS_UNUSABLE = 1;
	public static final int STATUS_INVALID = 2;

	/**
	 * return hostname as String representation.
	 */
	public StoreNode()
	{
	};

	public StoreNode(String name, String sn, String model, String mngtIp, int status)
	{
		this.name = name;
		this.sn = sn;
		this.model = model;
		this.status = status;
		this.mngtIp = mngtIp;
	};

	@Override
	public String toString()
	{
		return name; // return hostname only, StringUtils.join use it to
						// concatenate nodes
	}

}
