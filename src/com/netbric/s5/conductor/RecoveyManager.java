package com.netbric.s5.conductor;

import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Shard;
import com.netbric.s5.orm.Status;
import com.netbric.s5.orm.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

public class RecoveyManager {
	static final Logger logger = LoggerFactory.getLogger(RecoveyManager.class);
	static RecoveyManager inst;
	public static RecoveyManager getInstance() {
		return inst;
	}
	public boolean beginRecoveryVolume(Volume v) {

		List<Shard> illhealthShards = S5Database.getInstance().where("status != ?", Status.OK).results(Shard.class);
		for(Shard s : illhealthShards) {
			try {
				recoveryShard(s);
			} catch (StateException e) {
				logger.error("Failed to recovery shard:{}, for:{}", Utils.delayFormat("0x%x", s.id), e);
			}
		}
		return true;
	}

	private void recoveryShard(Shard s) throws StateException {
		class RepExt {
			public long id;
			public String store_ip;
			public int is_primary;
			public String status;
		}
		List<RepExt> replicas = S5Database.getInstance().where("shard_id=?", s.id).results(RepExt.class);
		RepExt primaryRep = null;
		for(RepExt r : replicas) {
			if(r.is_primary == 1){
				primaryRep = r;
				replicas.remove(r);
				break;
			}
		}
		if(primaryRep == null || primaryRep.status != Status.OK) {
			throw new StateException(String.format("Shard:%x has no primary replica available", s.id));
		}
	}
}
