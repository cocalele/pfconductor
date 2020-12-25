package com.netbric.s5.conductor.handler;

import com.dieselpoint.norm.DbException;
import com.dieselpoint.norm.Transaction;
import com.netbric.s5.conductor.InvalidParamException;
import com.netbric.s5.conductor.Main;
import com.netbric.s5.conductor.Utils;
import com.netbric.s5.conductor.VolumeIdUtils;
import com.netbric.s5.conductor.rpc.ErrorReportReply;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Status;
import com.netbric.s5.orm.Volume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.SQLException;

public class ErrorHandler {
	static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);
	public RestfulReply handleError(HttpServletRequest request, HttpServletResponse response) throws InvalidParamException {
		long repId = Utils.getParamAsLong(request, "rep_id");
		int sc = Utils.getParamAsInt(request, "sc");

		logger.error("SET_REPLICA_STATUS_ERROR_ Set replica:0x{} to ERROR status, for sc:{} from IP:{}", Long.toHexString(repId),
				sc, request.getRemoteAddr());


		long volId = VolumeIdUtils.replicaToVolumeId(repId);
		long shardId = VolumeIdUtils.replicaToShardId(repId);
		Transaction trans = S5Database.getInstance().startTransaction();
		try {
			S5Database.getInstance().transaction(trans).sql("select * from t_volume where id=? for update", volId).first(Volume.class);
			int changed = S5Database.getInstance().transaction(trans) //never set the last replica to ERROR status
					.sql("update t_replica set status=IF((select count(*) from t_replica where status='OK' and volume_id=?) > 1, 'ERROR', status) where id=?",
							volId, repId).execute().getRowsAffected();
			logger.info("{} replicas was set to ERROR state", changed);
			if(changed > 0) {
				long primaryIndex = S5Database.getInstance().queryLongValue("select primary_rep_index from t_shard where id=?", shardId);
				if(primaryIndex == repId) {
					S5Database.getInstance().transaction(trans).sql("update t_shard set primary_rep_index=" +
							"(select replica_index  from t_replica where shard_id=? and status='OK' " +
								"order by status_time asc, replica_index asc limit 1) " +
							" where id=? ", shardId, shardId).execute();
				}
				S5Database.getInstance().transaction(trans).sql("update t_shard set status=? where id=?", Status.DEGRADED, shardId).execute();
				changed = S5Database.getInstance().transaction(trans).sql(
						"update t_volume set status=? where id=?", Status.DEGRADED, volId)
						.execute().getRowsAffected();
				if(changed > 0) {
					logger.warn("Volume:{} status changed due to error handling", volId);
				}

			}
			trans.commit();
			long newMeta = S5Database.getInstance().queryLongValue("select meta_ver from t_volume where id=?", volId);
			return new ErrorReportReply("handle_error_reply", ErrorReportReply.ACTION_REOPEN, (int)newMeta);
		}
		catch(DbException | SQLException e) {
			trans.rollback();
			logger.error("Failed uddate DB in handle error and will suicide ... \n, {}", e);
			Main.suicide();
		}
		return new RestfulReply("handle_error_reply", -1, "Unexpected code branch");
	}

}
