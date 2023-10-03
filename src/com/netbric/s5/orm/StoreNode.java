package com.netbric.s5.orm;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.sql.SQLException;

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


	/**
	 * return hostname as String representation.
	 */
	public StoreNode()
	{
	};

	public static StoreNode fromId(long id) {
		return S5Database.getInstance().sql("select v.* from t_store v where id=?", id).first(StoreNode.class);
	}

	@Transient
	public String getRepPorts() throws SQLException {
		return S5Database.getInstance().queryStringValue(" select group_concat(ip_addr)"+
						" from  t_port where purpose=? and store_id=? ",
				Port.REPLICATING, id);
	}
	@Override
	public String toString()
	{
		return name; // return hostname only, StringUtils.join use it to
						// concatenate nodes
	}
}
