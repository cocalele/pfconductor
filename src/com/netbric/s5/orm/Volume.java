package com.netbric.s5.orm;

import javax.persistence.*;
import java.util.List;
// a replica id is composed as:
// bit[63..24]  volume index, generated by conductor
// bit[23..4] shard index
// bit[3..0] replica index

@Table(name = "t_volume")
public class Volume
{
	/**
	 * a replica id is composed as:
	 * bit[63..24]  volume index, generated by conductor
	 * bit[23..4] shard index
	 * bit[3..0] replica index
	 * while the volume id is: <volume_index> << 24
	 */
	@Id
	public long id;
	public String name;// varchar(96),
	public long size;
	public int iops;
	public int cbs;
	public long bw;
	public String status;
	@OneToOne
	@JoinColumn(name = "id")
	@JoinTable(name="t_tenant")
	public int tenant_id;
	public int quotaset_id;
	public int exposed; //1 means exposed, 0 means not
	public int rep_count; //replica count, 1, 2, 3
	public long shard_size; //shard size, default 64G
	public int meta_ver;
	public int snap_seq;
	public long features;
	public static final long FEATURE_AOF = 1; //this volume can only be used as AOF
	public static final long FEATURE_PFS2 = 2;

	public Volume()
	{
	};

	public static Volume fromName(String tenant_name, String volume_name) {
		return S5Database.getInstance().sql("select v.* from t_volume as v, t_tenant as t where t.id=v.tenant_id and t.name=? and v.name=?",
				tenant_name, volume_name).first(Volume.class);
	}
	public static Volume fromId(long id) {
		Volume v = S5Database.getInstance().sql("select v.* from t_volume v where id=?", id).first(Volume.class);
		return v;
	}

}
