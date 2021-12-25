package com.netbric.s5.conductor.handler;

import com.dieselpoint.norm.DbException;
import com.dieselpoint.norm.Transaction;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.conductor.*;
import com.netbric.s5.conductor.rpc.*;
import com.netbric.s5.orm.*;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

public class VolumeHandler
{
	public static class ReplicaArg
	{
		public long id;
		public long index;
		public long store_id;
		public String tray_uuid;
		public String status;
		public String rep_ports;
		public ReplicaArg clone()
		{
			ReplicaArg r  = new ReplicaArg();
			r.id = id;
			r.index = index;
			r.store_id = store_id;
			r.tray_uuid = tray_uuid;
			r.status = status;
			r.rep_ports = rep_ports;
			return r;
		}
	};
	public static class ShardArg
	{
		public long index;
		public List<ReplicaArg> replicas;
		public long primary_rep_index;
		public String status;
	};

	public static class PrepareVolumeArg
	{
		public String op;
		public String status;
		public String volume_name;
		public long volume_size;
		public long volume_id;
		public int shard_count;
		public int rep_count;
		public int meta_ver;
		public int snap_seq;
		public List<ShardArg> shards;
		public long features;
	}

	public static class ShardInfoForClient
	{
		public long index;
		public String store_ips;
		public String status;
		public ShardInfoForClient(){}
	};
	public static class OpenVolumeReply extends RestfulReply
	{
		public String status;
		public String volume_name;
		public long volume_size;
		public long volume_id;
		public int shard_count;
		public int rep_count;
		public int meta_ver;
		public int snap_seq;
		public ShardInfoForClient[] shards;

		public OpenVolumeReply(String op, int retCode, String reason) {
			super(op, retCode, reason);
		}
	}

	public static class ExposeVolumeReply extends RestfulReply
	{
		public ExposeVolumeReply(String op, MountTarget[] tgts){
			super(op);
			targets = tgts;
		}
		MountTarget[] targets;
	}

	static final Logger logger = LoggerFactory.getLogger(VolumeHandler.class);

	public RestfulReply expose_volume(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;
		String tenant_passwd;
		try
		{
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name");
			tenant_passwd = Utils.getParamAsString(request, "tenant_passwd", "123456");
		}
		catch (InvalidParamException e1)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());
		}
		Volume vol = S5Database.getInstance()
				.sql("select t_volume.* from t_volume, t_tenant where t_tenant.name=? and t_volume.name=? and t_volume.tenant_id=t_tenant.id",
						tenant_name, volume_name)
				.first(Volume.class);
		if (vol == null)
			return new RestfulReply(op, RetCode.INVALID_ARG,
					"Volume:" + tenant_name + ":" + volume_name + " not exists");
		if (!vol.status.equals(Status.OK))
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Volume:" + tenant_name + ":" + volume_name + " in status (" + vol.status + ") can't be exposed");
		if (vol.exposed != 0)
			return new RestfulReply(op, RetCode.ALREADY_DONE,
					"Volume:" + tenant_name + ":" + volume_name + " has already been exposed");

		List<Replica> reps = S5Database.getInstance().where("volume_id=? and status!=?", vol.id, Status.ERROR)
				.orderBy("status asc").results(Replica.class);
		if (reps.size() == 0)
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Volume:" + tenant_name + ":" + volume_name + " no replicas available");

		try
		{
			MountTarget[] tgts = NbdxServer.exposeVolume(vol, reps);

			RestfulReply r = new ExposeVolumeReply(op, tgts);
			return r;
		}
		catch (OperateException | IOException e)
		{
			return new RestfulReply(op, RetCode.REMOTE_ERROR, e.getMessage());
		}

	}
	public RestfulReply unexpose_volume(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;
		String tenant_passwd;
		try
		{
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name");
			tenant_passwd = Utils.getParamAsString(request, "tenant_passwd", "123456");
		}
		catch (InvalidParamException e1)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());
		}
		Volume vol = S5Database.getInstance()
				.sql("select t_volume.* from t_volume, t_tenant where t_tenant.name=? and t_volume.name=? and t_volume.tenant_id=t_tenant.id",
						tenant_name, volume_name)
				.first(Volume.class);
		if (vol == null)
			return new RestfulReply(op, RetCode.INVALID_ARG,
					"Volume:" + tenant_name + ":" + volume_name + " not exists");
		if (!vol.status.equals(Status.OK))
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Volume:" + tenant_name + ":" + volume_name + " in status (" + vol.status + ") can't be exposed");

		List<Replica> reps = S5Database.getInstance().where("volume_id=? and status!=?", vol.id, Status.ERROR)
				.orderBy("status asc").results(Replica.class);
		if (reps.size() == 0)
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Volume:" + tenant_name + ":" + volume_name + " no replicas available");

		try
		{
			MountTarget[] tgts = NbdxServer.unexposeVolume(vol, reps);

			RestfulReply r = new ExposeVolumeReply(op, tgts);
			return r;
		}
		catch (OperateException | IOException e)
		{
			return new RestfulReply(op, RetCode.REMOTE_ERROR, e.getMessage());
		}

	}

	public RestfulReply create_volume(HttpServletRequest request, HttpServletResponse response)
	{


		String op = request.getParameter("op");
		long volume_size = 0;

		Transaction trans = null;
		try
		{
			String tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			String volume_name = Utils.getParamAsString(request, "volume_name");
			volume_size = Utils.getParamAsLong(request, "size", 4L << 30) ;
			int  rep_count = Utils.getParamAsInt(request, "rep_cnt", 1);
			int iops = Utils.getParamAsInt(request, "iops", 8 << 10);
			long bw = Utils.getParamAsInt(request, "bw", 160 << 20);

			if(rep_count < 1 || rep_count >3)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG,
						String.format("rep_count:%d is invalid to create volume:%s. validate value shoule be 1,2,3", rep_count, volume_name));
			}

			RestfulReply r;
			if(op.equals("create_volume"))
				r = do_create_volume(volume_size, tenant_name, volume_name, rep_count, iops, bw, 0);
			else if(op.equals("create_aof"))
				r = do_create_volume(volume_size, tenant_name, volume_name, rep_count, iops, bw, Volume.FEATURE_AOF);
			else
				return new RestfulReply(op, RetCode.INVALID_ARG, String.format("Invalid OP:%s", op));
			r.op = op + "_reply";
			return r;
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}

	}

	private RestfulReply do_create_volume(long volume_size, String tenant_name, String volume_name, int rep_count, int iops, long bw, long feature)
	{
		String[] store_name;
		store_name = new String[3];
		int[] store_idx = new int[3];
		String[] tray_ids = new String[3];
		long usable_size = 0;
		Transaction trans = null;
		try {
			trans = S5Database.getInstance().startTransaction();
			long used_size;
			Volume v = new Volume();
			v.features = feature;
			Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).transaction(trans).first(Tenant.class);
			if (t == null) {
				return new RestfulReply(null, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);
			}

			Volume volume = S5Database.getInstance().table("t_volume")
					.where("name=? AND tenant_id=?", volume_name, t.id).transaction(trans).first(Volume.class);
			if (volume != null) {
				return new RestfulReply(null, RetCode.INVALID_ARG, "volume already exists: " + volume_name);
			}


			if (t.size > 0) {
				int count = S5Database.getInstance().sql("select count(*) from t_volume where tenant_id=?", t.id).transaction(trans)
						.first(Long.class).intValue();

				if (count != 0) {
					HashMap m = S5Database.getInstance().sql("select sum(size) as used from t_volume where tenant_id=?", t.id).transaction(trans)
							.first(HashMap.class);
					Object o = m.get("used");
					used_size = ((BigDecimal) o).longValue();
					usable_size = t.size - used_size;
					if (volume_size > usable_size)
						return new RestfulReply(null, RetCode.INVALID_ARG,
								"tenant: " + tenant_name + " has no enough volume capacity to create new volume: "
										+ volume_name + ", expected: " + volume_size + ", available: " + usable_size);
				}

				if (volume_size > t.size)
					return new RestfulReply(null, RetCode.INVALID_ARG,
							"tenant: " + tenant_name + " has no enough volume capacity to create new volume: " + volume_name
									+ ", expected: " + volume_size + ", available: " + usable_size);

			}

			v.id = S5Database.getInstance().queryLongValue("select NEXTVAL(seq_gen)  as val") << 24;
			v.rep_count = rep_count;
			v.name = volume_name;
			v.size = volume_size;
			v.shard_size = Config.DEFAULT_SHARD_SIZE;
			v.iops = iops;
			v.bw = bw;
			v.cbs = t.iops * 2;
			v.tenant_id = (int) t.id;
			v.status = Status.OK;
			v.snap_seq = 1;
			select_store(trans, v.rep_count, volume_size, store_name, tray_ids, store_idx);

			S5Database.getInstance().transaction(trans).insert(v);
			long shardCount = (v.size + v.shard_size - 1) / v.shard_size;
			for (int shardIndex = 0; shardIndex < shardCount; shardIndex++) {
				Shard shard = new Shard();
				shard.id = v.id | (shardIndex << 4);
				shard.primary_rep_index = 0;
				shard.volume_id = v.id;
				shard.status = Status.OK;
				shard.status_time = Timestamp.valueOf(LocalDateTime.now());
				S5Database.getInstance().insert(shard);
				for (int i = 0; i < v.rep_count; i++) {
					Replica r = new Replica();
					r.id = shard.id | i;
					r.shard_id = shard.id;
					r.volume_id = v.id;
					r.store_id = store_idx[i];
					r.tray_uuid = tray_ids[i];
					r.status = Status.OK;
					r.replica_index = i;
					r.status_time = Timestamp.valueOf(LocalDateTime.now());
					S5Database.getInstance().transaction(trans).insert(r);
				}
			}
			trans.commit();
			return new CreateVolumeReply(null, v);
		}
		catch (InvalidParamException | SQLException e)
		{
			trans.rollback();;
			return new RestfulReply(null, RetCode.INVALID_ARG, e.getMessage());
		}
	}

	private void select_store(Transaction trans, int replica_count, long volume_size, String[] store_names,
			String[] tray_ids, int[] store_ids) throws InvalidParamException {
		if (replica_count < 1 && replica_count > 3)
		{
			throw new InvalidParamException(String.format("invalid replica count:%d", replica_count));
		}

		boolean store_specified = false;
		boolean tray_specified = false;
		for (int i = 0; i < replica_count; i++)
		{
			if (StringUtils.isEmpty(store_names[i]) && store_specified == true)
			{
				throw new InvalidParamException("replica count is" + replica_count + "but num." + i + "store is not specified");
			}

			if (store_names[i] != null)
				store_specified = true;

			if (tray_ids[i] == null && tray_specified == true)
			{
				throw new InvalidParamException("replica count is" + replica_count + "but num." + i + "store is not specified");
			}

			if (tray_ids[i] != null)
				tray_specified = true;
		}

		if (!store_specified && tray_specified)
		{
			throw new InvalidParamException("if user choose to specify stores when create volume, trays must be also specified");
		}

		store_ids[0] = -1;
		if (store_specified)
		{
			// caller specify tray to use, just return store_ids^M
			int store_num = 0;
			for (int i = 0; i < 3; i++)
			{
				if (store_names[i] != null)
					store_num++;
			}
			if (replica_count != store_num)
			{
				throw new InvalidParamException("replica count is not equal to store count");
			}

			for (int i = 0; i < replica_count; i++)
			{
				StoreNode s = S5Database.getInstance().table("t_store")
						.where("name=? AND status=?", store_names[i], StoreNode.STATUS_OK).transaction(trans).first(StoreNode.class);
				store_ids[i] = s.id;
			}

			if (tray_specified)
			{
				for (int i = 0; i < replica_count; i++)
				{
					S5Database.getInstance()
							.sql("select t_tray.id from t_tray, v_tray_free_size where name=? and t_tray.store_id=? and "
									+ " t_tray.status='OK' and v_tray_free_size.free_size>=? and v_tray_free_size.store_id=t_tray.store_id "
									+ "and v_tray_free_size.tray_uuid=t_tray.uuid ",
							"TRAY-" + tray_ids[i], store_ids[i], volume_size).transaction(trans).first(Integer.class);
				}
				return;
			}
		}

		select_suitable_store_tray(trans, replica_count, volume_size, store_names, store_ids, tray_ids);
		return;
	}

	private void select_suitable_store_tray(Transaction trans, int replica_count, long volume_size,
			String[] store_names, int[] store_ids, String[] tray_ids) throws InvalidParamException {
		if (store_ids[0] == -1)
		{
			// the following SQL:
			// 1. t.free_size > volume_size, filter only the tray with
			// sufficient space
			// 2. max(t.free_size) , get the tray with most space
			// 3. group by store_id, each store to get one record
			// 4. order by s.free_size desc, order s5 store by its free size
			List<HashMap> list = S5Database.getInstance()
					.sql("select t.store_id,t.tray_uuid,max(t.free_size) as max_tray, "
							+ "s.free_size as store_free from v_tray_free_size as t,v_store_free_size as s "
							+ "where t.store_id = s.store_id and t.free_size>=? and t.status='OK' "
							+ "group by t.store_id order by s.free_size desc limit 3 ", volume_size).transaction(trans)
					.results(HashMap.class);

			if (list.size() < replica_count)
				throw new InvalidParamException("only " + list.size()
						+ " stores has tray with capacity over " + volume_size + "(byte) but replica is " + replica_count);

			// now choose tray for replica

			for (int i = 0; i < list.size(); i++)
			{
				HashMap h = list.get(i);
				store_ids[i] = (int) h.get("store_id");
				tray_ids[i] = (String) h.get("tray_uuid");

			}
		}
		else
		{
			for (int i = 0; i < replica_count; i++)
			{
				List<HashMap> list = S5Database.getInstance()
						.sql("select store_id, tray_uuid, max(free_size),0 from v_tray_free_size where free_size >=? and store_id=? and status='OK'",
								volume_size, store_ids[i]).transaction(trans)
						.results(HashMap.class);

				if (list.size() < 1)
					throw new InvalidParamException("store user specified for replica" + i + "has no tray with capacity over" + volume_size);

				for (HashMap h : list)
					tray_ids[i] = (String) h.get("tray_uuid");
			}
		}
	}

	public static class TempRep {
		public long replica_id;
		public String tray_uuid;
		public String mngt_ip;
	}
	public RestfulReply delete_volume(HttpServletRequest request, HttpServletResponse response)
	{

		// the following SQL:
		// 1. delete the replicas of the volume
		// 2.delete the volumes
		String op = request.getParameter("op");
		String tenant_name = null;
		String volume_name = null;
		int t_idx = 0;
		try
		{
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			Tenant tenant = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (tenant == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);

			volume_name = Utils.getParamAsString(request, "volume_name");
			Volume volume = S5Database.getInstance().table("t_volume").where("name=? and tenant_id=?",
					volume_name, tenant.id).first(Volume.class);
			if (volume == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume not exists: " + volume_name);


			t_idx = (int)tenant.id;
			List<TempRep> replicas = S5Database.getInstance().sql("select r.id replica_id, r.tray_uuid, s.mngt_ip " +
					"from t_replica r, t_store s where r.store_id=s.id and r.volume_id=?", volume.id).results(TempRep.class);
			for(TempRep r : replicas) {
				logger.info("Deleteing replica:{} on store:{} disk:{}",  Long.toHexString(r.replica_id), r.mngt_ip, r.tray_uuid);
				try {
					SimpleHttpRpc.invokeStore(r.mngt_ip, "delete_replica", RestfulReply.class, "replica_id", r.replica_id,
							"ssd_uuid", r.tray_uuid);
				} catch (Exception e1) {
					logger.error("Failed delete replica:{}, {}", Long.toHexString(r.replica_id), e1);
				}
			}
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		int rowaffected = S5Database.getInstance()
				.sql("delete from t_replica where t_replica.volume_id in (select t_volume.id from t_volume where name='"
						+ volume_name + "' and tenant_id=" + t_idx + ")")
				.execute().getRowsAffected();
		int r = S5Database.getInstance().table("t_volume").where("name=? AND tenant_id=?", volume_name, t_idx).delete()
				.getRowsAffected();
		if (r == 1)
			return new RestfulReply(op);
		return new RestfulReply(op, RetCode.INVALID_ARG, "Deleted volumes:" + r + "delete replica" + rowaffected);
	}

	public RestfulReply update_volume(HttpServletRequest request, HttpServletResponse response)
	{

		Volume nv = null;
		String volume_name = null;
		boolean DataLost = false;

		String op = request.getParameter("op");
		try
		{
			String tenant_name = Utils.getParamAsString(request, "tenant_name");
			Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (t == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists:" + tenant_name);

			int idx = (int)t.id;

			volume_name = Utils.getParamAsString(request, "volume_name");
			Volume volume = S5Database.getInstance().table("t_volume").where("name=?", volume_name).first(Volume.class);
			if (volume == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume not exists:" + volume_name);

			nv = S5Database.getInstance().table("t_volume").where("name=? AND tenant_id=?", volume_name, idx)
					.first(Volume.class);
			nv.name = Utils.getParamAsString(request, "new_volume_name", nv.name);

			long size = 0;

			try
			{
				size = Utils.getParamAsLong(request, "size") * 1024 * 1024;
				if (size < nv.size)
					DataLost = true;
			}
			catch (Exception e)
			{
				size = nv.size;
			}
			nv.size = size;

			int iops = 0;
			try
			{
				iops = Utils.getParamAsInt(request, "iops") * 1024;
			}
			catch (Exception e)
			{
				iops = nv.iops;
			}
			nv.iops = iops;

			long bw = 0;
			try
			{
				bw = Utils.getParamAsInt(request, "bw") * 1024 * 1024;
			}
			catch (Exception e)
			{
				bw = nv.bw;
			}

			nv.bw = bw;
			nv.cbs = 2 * iops;
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		S5Database.getInstance().update(nv);
		if (DataLost == true)
			return new RestfulReply(op, RetCode.OK, "the date may be lost");
		else
			return new RestfulReply(op);
	}

	static class ListVolumeReply  extends RestfulReply
    {
        public ListVolumeReply(String op, List<Volume> vols)
        {
            super(op);
            volumes = vols;
        }
        public List<Volume> volumes;
    }
	public RestfulReply list_volume(HttpServletRequest request, HttpServletResponse response)
	{

		String op = request.getParameter("op");
		String tenant_name = null;
		int limit = 0;
		tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
		Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
		if (t == null)
			return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);

		limit = Utils.getParamAsInt(request, "limit", 20);

		int tenant_idx = t.id;
		String vol_name = Utils.getParamAsString(request, "name", "");
		List<Volume> volumes;
		if(vol_name != null && vol_name.length() > 0) {
			volumes = S5Database.getInstance()
					.sql("select * from t_volume where tenant_id=? and name=?", tenant_idx, vol_name).results(Volume.class);
		}else {
			volumes = S5Database.getInstance()
					.sql("select * from t_volume where tenant_id=?", tenant_idx).results(Volume.class);
		}

		if (limit >= volumes.size())
			limit = volumes.size();


		RestfulReply reply = new ListVolumeReply(request.getParameter("op"), volumes);
		return reply;

	}

	public static PrepareVolumeArg getPrepareArgs(String tenant_name, String volume_name) throws InvalidParamException, StateException
	{
		Volume vol = S5Database.getInstance()
				.sql("select t_volume.* from t_volume, t_tenant where t_tenant.name=? and t_volume.name=? and t_volume.tenant_id=t_tenant.id",
						tenant_name, volume_name)
				.first(Volume.class);
		if (vol == null)
			throw new InvalidParamException("Volume:" + tenant_name + ":" + volume_name + " not exists");
		if (vol.status.equals(Status.ERROR))
			throw new InvalidParamException("Volume:" + tenant_name + ":" + volume_name + " in status (" + vol.status + ") can't be opened");
		return getPrepareArgs(vol);
	}

	public static PrepareVolumeArg getPrepareArgs(Volume vol) throws  StateException
	{
		List<Shard> shards = S5Database.getInstance().where("volume_id=?", vol.id).results(Shard.class);

		PrepareVolumeArg arg = new PrepareVolumeArg();
		arg.status = vol.status;
		arg.volume_name = vol.name;
		arg.volume_size = vol.size;
		arg.volume_id =vol.id;
		arg.shard_count = shards.size();//TODO: not support shard yet
		arg.rep_count=vol.rep_count;
		arg.meta_ver = vol.meta_ver;
		arg.snap_seq = vol.snap_seq;
		arg.shards = new ArrayList<>(arg.shard_count);
		arg.features = vol.features;

		for(int i=0;i<arg.shard_count;i++)
		{
			Shard dbShard = shards.get(i);
			ShardArg shard = new ShardArg();
			shard.index=dbShard.shard_index;
			shard.status = dbShard.status;
			shard.primary_rep_index = dbShard.primary_rep_index;
			long shardId = dbShard.id;

			shard.replicas = S5Database.getInstance().sql("select r.replica_id as id, r.replica_index as 'index', r.tray_uuid, r.store_id, r.status, r.store_id, r.status, r.rep_ports " +
					" from v_replica_ext r where r.shard_id=?", shardId)
					.results(ReplicaArg.class);
			if (shard.replicas.size() == 0)
				throw new StateException("Volume:" + vol.tenant_id + ":" + vol.name + " no replicas available");
			arg.shards.add(shard);
		}
		return arg;
	}
	public static RestfulReply prepareShardsOnStore(StoreNode s, VolumeHandler.PrepareVolumeArg arg) throws Exception
	{
		return prepareOnStore("prepare_shards", s, arg);
	}

	public static RestfulReply prepareVolumeOnStore(StoreNode s, VolumeHandler.PrepareVolumeArg arg) throws Exception
	{
		return prepareOnStore("prepare_volume", s, arg);
	}
	private static RestfulReply prepareOnStore(String op, StoreNode s, VolumeHandler.PrepareVolumeArg arg) throws Exception
	{
		arg.op="prepare_volume";
		GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
		Gson gson = builder.create();
		String jsonStr = gson.toJson(arg);

		logger.info("Prepare volume:{} on node:{}, {}", arg.volume_name, s.mngtIp, jsonStr);
		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		try {
			client.start();
			ContentResponse response = client.newRequest(String.format("http://%s:49181/api?op=%s&name=%s",
					s.mngtIp, op, URLEncoder.encode(arg.volume_name, StandardCharsets.UTF_8.toString())))
					.method(org.eclipse.jetty.http.HttpMethod.POST)
					.content(new org.eclipse.jetty.client.util.StringContentProvider(jsonStr), "application/json")
					.send();
			logger.info("Get response:{}", response.getContentAsString());
			if(response.getStatus() < 200 || response.getStatus() >= 300)
			{
				throw new IOException(String.format("Failed to prepare_volume:%s on node:%s, HTTP status:%d, reason:%s",
						arg.volume_name, s.mngtIp, response.getStatus(), response.getReason()));
			}
			RestfulReply r = gson.fromJson(new String(response.getContent()), RestfulReply.class);
			client.stop();
			if(r.retCode == RetCode.OK)
				logger.info("Succeed {}:{} on node:{}", op, arg.volume_name, s.mngtIp);
			else
				logger.error("Failed to {}:{} on node:%s, code:%d, reason:{}", op, arg.volume_name, s.mngtIp, r.retCode, r.reason);
			return r;
		} catch (Exception e) {
			throw e;
		}
		finally {
			client.destroy();
		}

	}

	private static void markReplicasOnStoreAsError(int storeId, long volumeId)
	{
		S5Database.getInstance().sql("update t_replica set status='ERROR' where volume_id=? and store_id=?", volumeId, storeId).execute();
	}

	private OpenVolumeReply getVolumeInfoForClient(long volumeId)
	{
		Volume v = S5Database.getInstance().where("id=?", volumeId).first(Volume.class);
		List<Port> ports = S5Database.getInstance().sql("select t_port.* from t_port, t_store where t_port.purpose=? and t_port.store_id=t_store.id and (t_store.status=? or t_store.status=?)",
				Port.DATA, Status.OK, Status.MAINTAIN).results(Port.class);
		HashMap<Integer, ArrayList<String>> portMap = new HashMap<>();
		for(Iterator<Port> it=ports.iterator();it.hasNext();)
		{
			Port p = it.next();
			if(portMap.containsKey(p.store_id))
				portMap.get(p.store_id).add(p.ip_addr);
			else {
				ArrayList<String> ips = new ArrayList<>(4);
				ips.add(p.ip_addr);
				portMap.put(p.store_id, ips);
			}
		}
		logger.info("{} ports found ", portMap.size());

		List<ShardInfoForClient> shards = S5Database.getInstance().sql("select s.status, s.shard_index as 'index'," +
				" (select group_concat(data_ports order by is_primary desc, status_time asc, replica_index asc)"+
				" from  v_replica_ext where shard_id=s.id and status='OK') as store_ips from t_shard as s where s.volume_id=?",
				volumeId).results(ShardInfoForClient.class);

		logger.info("shard cnt:{}", shards.size());
		for(ShardInfoForClient sd : shards) {
			if(sd.store_ips == null)
			{
				sd.store_ips = "";
				sd.status = "ERROR";
			}
			logger.info("store_ips:{}", sd.store_ips);
		}
		OpenVolumeReply reply = new OpenVolumeReply("open_volume", RetCode.OK, null);
		reply.volume_id=v.id;
		reply.volume_name=v.name;
		reply.rep_count=v.rep_count;
		reply.volume_size=v.size;
		reply.status = v.status;
		reply.meta_ver = v.meta_ver;
		reply.snap_seq = v.snap_seq;
		reply.shards = new ShardInfoForClient[shards.size()];
		shards.toArray(reply.shards);
		return reply;
	}

	public static PrepareVolumeArg prepareVolume(String tenant_name, String volume_name, long feature) throws InvalidParamException, StateException, LoggedException {
		boolean need_reprepare;
		PrepareVolumeArg arg = null;
		do {
			need_reprepare = false;
			arg = getPrepareArgs(tenant_name, volume_name);
			if (arg.status == Status.ERROR) {
				logger.error(String.format("Failed to open volume:%s for it's in ERROR state", volume_name));
				throw new StateException(String.format("Failed to open volume:%s for it's in ERROR state", volume_name));
			}
			if(arg.features != feature)	{
				throw new LoggedException(logger, "Feature request for volume:%s not match, request:0x%lx volume has:0x%lx", volume_name, feature, arg.features);
			}
			List<StoreNode> stores = S5Database.getInstance()
					.sql("select t_store.* from t_store where id in (select distinct store_id from t_replica where volume_id=?)  and status='OK'",
							arg.volume_id).results(StoreNode.class);

			for (Iterator<StoreNode> it = stores.iterator(); it.hasNext(); ) {
				StoreNode s = it.next();
				if(!s.status.equals(Status.OK)){
					logger.warn("Prepare volume:{} on store:{}, but store status is:{}", volume_name, s.id, s.status);
				}
				RestfulReply rply = null;
				try {
					rply = prepareVolumeOnStore(s, arg);
				} catch (Exception e) {
					logger.error("Failed[2] to prepare volume {} on store:{}, for:{}", volume_name, s.mngtIp, e);
					markReplicasOnStoreAsError(s.id, arg.volume_id);
					need_reprepare = true;
					break;
				}
				if (rply.retCode != RetCode.OK) {
					logger.error(String.format("Failed to prepare volume %s on store:%s, for:%s", volume_name, s.mngtIp, rply.reason));
					throw new StateException(String.format("Failed to prepare volume %s on store:%s, for:%s", volume_name, s.mngtIp, rply.reason));
				}
			}
		} while (need_reprepare);
		return arg;
	}

	public RestfulReply open_volume(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;
		String snapshot_name;
		Snapshot snapshot;
		long volumeId;
		try {
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			snapshot_name = Utils.getParamAsString(request, "snapshot_name", null);
			long feature_request = 0;
			if(op.equals("open_aof")) {
				feature_request = Volume.FEATURE_AOF;
			}
			PrepareVolumeArg arg = prepareVolume(tenant_name, volume_name, feature_request);
			volumeId = arg.volume_id;
		} catch (InvalidParamException | StateException  e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());
		}

		OpenVolumeReply r = getVolumeInfoForClient(volumeId);
		if(snapshot_name!=null) {
			snapshot = S5Database.getInstance().where("volume_id=? and name=?", volumeId, snapshot_name).first(Snapshot.class);
			r.snap_seq = (int)snapshot.snap_seq;
		} else {
			r.snap_seq=-1;
		}
		return r;
	}

	public RestfulReply createSnapshot(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;
		String snap_name;
		long volumeId;
		Transaction t = null;
		Volume v = null;
		Snapshot snap = null;
		try {
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			snap_name = Utils.getParamAsString(request, "snapshot_name");
			v = Volume.fromName(tenant_name, volume_name);
			if(v == null)
				throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
			snap = Snapshot.fromName(tenant_name, volume_name, snap_name);
			if(snap != null)
				throw new InvalidParamException(String.format("Snapshot %s already exists", snap_name));

			t = S5Database.getInstance().startTransaction();
			v = S5Database.getInstance().sql("select * from t_volume where id=? for update", v.id).transaction(t).first(Volume.class);//lock db
			snap = new Snapshot();
			snap.name = snap_name;
			snap.size = v.size;
			snap.volume_id = v.id;
			snap.snap_seq = v.snap_seq;
			S5Database.getInstance().transaction(t).insert(snap);
			S5Database.getInstance().transaction(t).sql("update t_volume set snap_seq=snap_seq+1 where id=?", v.id)
				.execute();
			t.commit(); //commit change to snap_seq, even later push snap_seq tot store node failed, we waste a snap_seq number only,
			            //this snap_seq number will never be reused.
		} catch (InvalidParamException e1) {
			if(t != null)
				t.rollback();;
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());

		}
		Volume v2 = Volume.fromId(v.id);//now get new meta_ver after snapshot
		boolean updateFailed = false;
		List<StoreNode> nodes = S5Database.getInstance()
				.sql("select * from t_store where id in (select distinct store_id from t_replica where volume_id=? and status=?)",
						v.id, Status.OK)
				.results(StoreNode.class);
		for(StoreNode n : nodes) {
			try {
				SimpleHttpRpc.invokeStore(n.mngtIp, "set_snap_seq", RestfulReply.class, "volume_id", v2.id, "snap_seq", v2.snap_seq);
			}
			catch(Exception e){
				updateFailed = true;
				updateReplicaStatusToError(n.id, v2.id, String.format("push snap_seq to store:%d fail", n.id));
				logger.error("Failed update snap_seq on store:{}({}), ", n.id, n.mngtIp);
			}
		}
		if(updateFailed) {
			incrVolumeMetaver(v2, true, "create snapshot failed");
		}
		return new RestfulReply(op);
	}

	static void updateReplicaStatusToError(int storeId, long volumeId, String reason)
	{
		logger.error("Update replicas to ERROR on store:{} of volume:{}, for:{}", storeId, volumeId, reason);
		S5Database.getInstance().sql("update t_replica set status=? where volume_id=? and store_id=?",
				Status.ERROR, volumeId, storeId).execute();

	}

	public static void incrVolumeMetaver(Volume v, boolean pushToStore, String reason)
	{
		S5Database.getInstance().sql("update t_volume set meta_ver=meta_ver+1 where id=?", v.id)
				.execute();
		Volume v2 = Volume.fromId(v.id);
		logger.warn("increase volume:{} metaver from {} to {}, for:{}",v.name, v.meta_ver, v2.meta_ver, reason);
		if(pushToStore){
			List<StoreNode> nodes = S5Database.getInstance()
					.sql("select * from t_store where id in (select distinct store_id from t_replica where volume_id=? and status=?)",
							v.id, Status.OK)
					.results(StoreNode.class);
			for(StoreNode n : nodes) {
				try {
					SimpleHttpRpc.invokeStore(n.mngtIp, "set_meta_ver", RestfulReply.class, "volume_id", v2.id, "meta_ver", v2.meta_ver);
				}
				catch(Exception e){
					logger.error("Failed to push meta_ver to store:{}, for:{}", n.mngtIp, e);
					updateReplicaStatusToError(n.id, v2.id, String.format("push meta_ver to store:%d fail", n.id));
					incrVolumeMetaver(v2, pushToStore, String.format("push meta_ver to store:%d fail", n.id));
				}
			}
		}
	}

	public RestfulReply recoveryVolume(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;

		try {
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");

			final Volume v = Volume.fromName(tenant_name, volume_name);
			if(v == null)
				throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
			BackgroundTaskManager.BackgroundTask t;

			t = BackgroundTaskManager.getInstance().initiateTask(
					BackgroundTaskManager.TaskType.RECOVERY, "recovery volume:" + volume_name,
					new BackgroundTaskManager.TaskExecutor() {
						public void run(BackgroundTaskManager.BackgroundTask t) throws Exception {
							prepareVolume(tenant_name, volume_name, v.features);
							RecoveryManager.getInstance().recoveryVolume(t);
						}
					}, v);
			return new BackgroundTaskReply(op+"_reply", t);
		} catch (InvalidParamException e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());

		}
	}

	public RestfulReply moveVolume(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;

		try {
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");

			final Volume v = Volume.fromName(tenant_name, volume_name);
			if(v == null)
				throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
			BackgroundTaskManager.BackgroundTask t;
			long targetStoreId = Utils.getParamAsLong(request, "target_store");
			long fromStoreId = Utils.getParamAsLong(request, "from_store");
			String targetSsdUuid = Utils.getParamAsString(request, "target_ssd_uuid");
			String fromSsdUuid = Utils.getParamAsString(request, "from_ssd_uuid");

			t = BackgroundTaskManager.getInstance().initiateTask(
					BackgroundTaskManager.TaskType.RECOVERY, "move volume:" + volume_name,
					t1 -> {
						prepareVolume(tenant_name, volume_name, v.features);
						RebalanceManager.getInstance().moveVolume(t1, fromStoreId, fromSsdUuid, targetStoreId, targetSsdUuid);
					}, v);
			return new BackgroundTaskReply(op+"_reply", t);
		} catch (InvalidParamException e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());

		}
	}

	public RestfulReply queryTask(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");

		try {
			long taskId = Utils.getParamAsLong(request, "task_id");

			BackgroundTaskManager.BackgroundTask t= BackgroundTaskManager.getInstance().taskMap.get((long)taskId);
			if(t != null) {
				logger.info("query task id:{} status:{}, toString():{}, name:{}", t.id, t.status, t.status.toString(), t.status.name());
				return new BackgroundTaskReply(op, t);
			}
			else {
				return new RestfulReply(op, RetCode.INVALID_ARG, String.format("No such task:%d", taskId));
			}
		} catch (InvalidParamException e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());

		}
	}

	public static void pushMetaverToStore(Volume vol) throws Exception
	{
		vol = Volume.fromId(vol.id);//requery volume for latest meta_ver
		List<StoreNode> stores = S5Database.getInstance().sql(" select * from t_store where id in (select store_id from t_replica where volume_id=?) " +
				"and status='OK'", vol.id)
				.results(StoreNode.class);
		logger.warn("push volume:{} new meta_ver:{} to store, ",vol.name, vol.meta_ver);

		for(StoreNode s : stores) {
			RestfulReply reply = SimpleHttpRpc.invokeStore(s.mngtIp, "set_meta_ver", RestfulReply.class,
					"volume_id", vol.id, "meta_ver", vol.meta_ver);
			if(reply.retCode != 0) {
				logger.error("update_metaver on node:{} failed, reason:{}", s.mngtIp, reply.reason);
				throw new Exception(reply.reason);
			}
		}
	}


	public static final class RepExtTemp{
		public RepExtTemp(){}
		public long replica_index;
		public long replica_id;
		public String tray_uuid;
		public String mngt_ip;
		public String  md5;
	};
	public static final class CalcMd5Reply  extends RestfulReply{
		public String md5;
		public CalcMd5Reply(String op) {
			super(op);
		}
	}
	public RestfulReply scrubVolume(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;

		Volume v = null;

		try {
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");

			v = Volume.fromName(tenant_name, volume_name);
			if(v == null)
				throw new InvalidParamException(String.format("Volume %s/%s not found", tenant_name, volume_name));
			Volume finalV = v;
			BackgroundTaskManager.BackgroundTask t=null;

			t = BackgroundTaskManager.getInstance().initiateTask(
					BackgroundTaskManager.TaskType.RECOVERY, "scrub volume:" + volume_name,
					new BackgroundTaskManager.TaskExecutor() {
						public void run(BackgroundTaskManager.BackgroundTask t) throws Exception {
							List<Shard> shards = S5Database.getInstance().where("volume_id=?", finalV.id).results(Shard.class);
							int progress = 1;
							int inconsist_cnt = 0;
							for(Shard s : shards) {
								List<RepExtTemp> reps = S5Database.getInstance().
										sql("select r.id replica_id, r.tray_uuid, replica_index, mngt_ip from t_replica r, t_store s " +
												" where r.store_id=s.id and r.shard_id=? and r.status=?", s.id, Status.OK).results(RepExtTemp.class);
								RepExtTemp primary = null;
								for(RepExtTemp r : reps) {
									CalcMd5Reply reply = SimpleHttpRpc.invokeStore(r.mngt_ip, "calculate_replica_md5", CalcMd5Reply.class,
											"replica_id", r.replica_id,
											"ssd_uuid", r.tray_uuid);
									if(reply.retCode != 0) {
										logger.error("Failed to get replica md5 from store:{} for: {}", r.mngt_ip, reply.reason);
									} else {
										logger.info("Get replica:0x{} md5:{}", Long.toHexString(r.replica_id), reply.md5);
									}
									if(r.replica_index == s.primary_rep_index)
										primary =r;
									r.md5 = reply.md5;

								}
								if(primary == null || primary.md5 == null ){
									logger.error("Primary replica not available for shard:0x{}", Long.toHexString(s.id));
									continue;
								}
								for(RepExtTemp r : reps) {
									if(!primary.md5.equals(r.md5)) {
										inconsist_cnt++;
										logger.error("Replica 0x:{} md5:{} diff to primary:{}", Long.toHexString(r.replica_id), r.md5, primary.md5);
										logger.error("SET_REPLICA_STATUS_ERROR_ Set replica:0x{} to ERROR status, for scrub", Long.toHexString(r.replica_id));
										Transaction trans = S5Database.getInstance().startTransaction();
										try {
											S5Database.getInstance().transaction(trans)
													.sql("update t_replica set status='ERROR' where id=?", r.replica_id).execute();
											S5Database.getInstance().transaction(trans).sql("update t_shard set status=? where id=?", Status.DEGRADED, s.id).execute();
											int changed = S5Database.getInstance().transaction(trans).sql(
													"update t_volume set status=? where id=?", Status.DEGRADED, finalV.id)
													.execute().getRowsAffected();
											if(changed > 0) {
												logger.warn("Volume:{} status changed due to scrub", finalV.id);
											}
											trans.commit();;
										}catch(DbException  e) {
											trans.rollback();
											logger.error("Failed uddate DB and will suicide ... \n, {}", e);
											Main.suicide();
										}
										pushMetaverToStore(finalV);
									}

								}

								t.progress = progress*100/shards.size();
								progress++;
							}
							logger.info("scrub volume:{}, id:0x{} complete, {} replicas are inconsistency", finalV.name,
									Long.toHexString(finalV.id), inconsist_cnt);
							t.status = (inconsist_cnt == 0 ? BackgroundTaskManager.TaskStatus.SUCCEEDED : BackgroundTaskManager.TaskStatus.FAILED);
						}
					}, v);
			return new BackgroundTaskReply(op+"_reply", t);
		} catch (InvalidParamException e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());

		}
	}
	public RestfulReply check_volume_exists(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;
		try{
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");

			long v = S5Database.getInstance().queryLongValue("SELECT EXISTS (select v.* from t_volume as v, t_tenant as t where t.id=v.tenant_id and t.name=? and v.name=?)",
					tenant_name, volume_name);
			if(v==1)
				return new RestfulReply(op, RetCode.OK, null);
			else
				return new RestfulReply(op, RetCode.VOLUME_NOT_EXISTS, null);
		} catch (InvalidParamException | SQLException e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());
		}
	}
}

