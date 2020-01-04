package com.netbric.s5.orm;

import com.netbric.s5.conductor.handler.VolumeHandler;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_store")
public class StoreNode
{
	@Id
	public int id;
	public String name;
	public String sn;
	public String model;
	public String status;
	@Column(name = "mngt_ip")
	public String mngtIp;

	// following values are copied from C definition to s5c_store_status
	public static final String STATUS_OK = "OK";
	public static final String STATUS_OFFLINE = "OFFLINE";
	public static final String STATUS_ERROR = "ERROR";

	/**
	 * return hostname as String representation.
	 */
	public StoreNode()
	{
	};

	@Override
	public String toString()
	{
		return name; // return hostname only, StringUtils.join use it to
						// concatenate nodes
	}
}
