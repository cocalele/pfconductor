package com.netbric.s5.orm;

import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_snapshot")
public class Snapshot {
	@Id
	public long id;
	public long volume_id;
	public long snap_seq;
	public String name;
	public long size;

	public static Snapshot fromName(String tenant_name, String volume_name, String snapshot_name) {
		return S5Database.getInstance().sql("select t_snapshot.* from t_snapshot, t_volume as v, t_tenant as t" +
						" where t.id=v.tenant_id and t.name=? and v.name=? and t_snapshot.volume_id=v.id and t_snapshot.name=?",
				tenant_name, volume_name, snapshot_name).first(Snapshot.class);
	}

}
