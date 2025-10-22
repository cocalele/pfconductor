package com.netbric.s5.conductor.handler;

import com.dieselpoint.norm.DbException;
import com.dieselpoint.norm.Transaction;
import com.netbric.s5.conductor.exception.InvalidParamException;
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

import com.netbric.s5.conductor.HTTPServer.Request;
import com.netbric.s5.conductor.HTTPServer.Response;
import java.io.IOException;
import java.sql.SQLException;

public class ErrorHandler {
	public static final int MSG_STATUS_NOT_PRIMARY = 0xC0;
	public static final int MSG_STATUS_NOSPACE = 0xC1;
	public static final int MSG_STATUS_READONLY = 0xC2;
	public static final int MSG_STATUS_CONN_LOST = 0xC3;
	public static final int MSG_STATUS_AIOERROR = 0xC4;
	public static final int MSG_STATUS_REP_TO_PRIMARY = 0xCE;
	public static final int MSG_STATUS_ERROR_HANDLED = 0xC5;
	public static final int MSG_STATUS_ERROR_UNRECOVERABLE = 0xC6;
	public static final int MSG_STATUS_AIO_TIMEOUT = 0xC7;
	public static final int MSG_STATUS_REPLICATING_TIMEOUT = 0xC8;
	public static final int MSG_STATUS_NODE_LOST = 0xC9;
	public static final int MSG_STATUS_LOGFAILED = 0xCA;
	static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);
	public RestfulReply handleError(Request request, Response response) throws InvalidParamException {
		long repId = Utils.getParamAsLong(request, "rep_id");
		int sc = Utils.getParamAsInt(request, "sc");
		switch(sc){
		case MSG_STATUS_NOT_PRIMARY:
			logger.error("replica:0x{} want to be primary from IP:{}", Long.toHexString(repId), request.getRemoteAddr());
			return switchPrimary(repId);
		case MSG_STATUS_REP_TO_PRIMARY:
			logger.error("Uuexcepted error, Replicate write to primary node, from primary:{}", request.getRemoteAddr());
		case MSG_STATUS_AIOERROR:
		case MSG_STATUS_CONN_LOST:
		case MSG_STATUS_NOSPACE:
		case MSG_STATUS_LOGFAILED:
			return markReplicaError(request, repId, sc);
		default:
			logger.error("Unknown error code:{} from IP:{}", sc, request.getRemoteAddr());
		}
		return new ErrorReportReply("handle_error_reply", ErrorReportReply.ACTION_REOPEN, 0);
		
	}

	private RestfulReply markReplicaError(Request request, long repId, int sc)
	{
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
				if (primaryIndex == VolumeIdUtils.replicaIdToIndex(repId)) {
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
		finally{
			if(trans != null){
				try {
					trans.getConnection().close();
				} catch (SQLException e) {
					logger.error("Failed close transaction", e);
				}
			}

		}
		return new RestfulReply("handle_error_reply", -1, "Unexpected code branch");
	}

	private ErrorReportReply switchPrimary(long repId) {
		long volId = VolumeIdUtils.replicaToVolumeId(repId);
		long shardId = VolumeIdUtils.replicaToShardId(repId);
		Transaction trans = S5Database.getInstance().startTransaction();
		try {
			S5Database.getInstance().transaction(trans).sql("select * from t_volume where id=? for update", volId).first(Volume.class);
			long primaryIndex = S5Database.getInstance().queryLongValue("select primary_rep_index from t_shard where id=?", shardId);
			long primaryRepId = shardId | primaryIndex;
			if(primaryRepId == repId) {
				logger.info("{} is already primary", Long.toHexString(repId));
				long newMeta = S5Database.getInstance().queryLongValue("select meta_ver from t_volume where id=?", volId);
				return new ErrorReportReply("handle_error_reply", ErrorReportReply.ACTION_REOPEN, (int)newMeta);
			}



			logger.error("SET_REPLICA_STATUS_ERROR_ Set replica:0x{} to ERROR status, for replica:0x{} receive client IO", Long.toHexString(primaryRepId), Long.toHexString(repId));

			int changed = S5Database.getInstance().transaction(trans) //never set the last replica to ERROR status
					.sql("update t_replica set status=IF((select count(*) from t_replica where status='OK' and volume_id=?) > 1, 'ERROR', status) where id=?",
							volId, primaryRepId).execute().getRowsAffected();
			logger.info("{} replicas was set to ERROR state", changed);
			if(changed > 0) {
				S5Database.getInstance().transaction(trans).sql("update t_shard set status=? where id=?", Status.DEGRADED, shardId).execute();
				changed = S5Database.getInstance().transaction(trans).sql("update t_volume set status=? where id=?", Status.DEGRADED, volId)
						.execute().getRowsAffected();
				if(changed > 0) {
					
					logger.warn("Volume:{} status changed to DEGRADED due to error handling", volId);
				}

			}

			S5Database.getInstance().transaction(trans).sql("update t_shard set primary_rep_index=? where id=? ", 
					VolumeIdUtils.replicaIdToIndex(repId), shardId).execute();

			
			trans.commit();
			long newMeta = S5Database.getInstance().queryLongValue("select meta_ver from t_volume where id=?", volId);
			return new ErrorReportReply("handle_error_reply", ErrorReportReply.ACTION_REOPEN, (int)newMeta);
		}
		catch(DbException | SQLException e) {
			trans.rollback();
			logger.error("Failed uddate DB in handle error and will suicide ... \n, {}", e);
			Main.suicide();
		}
		finally{
			if(trans != null){
				try {
					trans.getConnection().close();
				} catch (SQLException e) {
					logger.error("Failed close transaction", e);
				}
			}

		}
		return new ErrorReportReply("handle_error_reply", ErrorReportReply.ACTION_REOPEN, "Unexpected code branch");
	}
}
