package com.netbric.s5.conductor.handler;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.conductor.*;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class VolumeHandler
{
	class ReplicaArg
	{
		public int index;
		public int store_id;
		public String tray_uuid;
	};
	class ShardArg
	{
		public int index;
		public List<ReplicaArg> replicas;
	};

	public class PrepareVolumeArg
	{
		public String op;
		public String status;
		public String volume_name;
		public long volume_size;
		public long volume_id;
		public int shard_count;
		public int rep_count;
		public List<ShardArg> shards;
	}

	class ShardInfoForClient
	{
		public int index;
		public List<String> store_ips;
	};
	public class OpenVolumeReply extends RestfulReply
	{
		public String op;
		public String status;
		public String volume_name;
		public long volume_size;
		public long volume_id;
		public int shard_count;
		public int rep_count;
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

	static final Logger logger = LoggerFactory.getLogger(S5RestfulHandler.class);

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
		if (vol.exposed)
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
		String[] store_name;
		store_name = new String[3];
		int[] store_idx = new int[3];
		String[] tray_ids = new String[3];


		String op = request.getParameter("op");
		long volume_size = 0;
		long used_size = 0;
		long usable_size = 0;

		Volume v = new Volume();

		try
		{
			String tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (t == null)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);
			}

			String volume_name = Utils.getParamAsString(request, "name");
			Volume volume = S5Database.getInstance().table("t_volume")
					.where("name=? AND tenant_id=?", volume_name, t.id).first(Volume.class);
			if (volume != null)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume already exists: " + volume_name);
			}

			volume_size = Utils.getParamAsLong(request, "size", 4L << 30) ;

			if(t.size > 0)
			{
			int count = S5Database.getInstance().sql("select count(*) from t_volume where tenant_id=?", t.id)
					.first(Long.class).intValue();

				if (count != 0)
				{
					HashMap m = S5Database.getInstance().sql("select sum(size) as used from t_volume where tenant_id=?", t.id)
								.first(HashMap.class);
					Object o = m.get("used");
					used_size = ((BigDecimal)o).longValue();
					usable_size = t.size - used_size;
					if (volume_size > usable_size)
						return new RestfulReply(op, RetCode.INVALID_ARG,
								"tenant: " + tenant_name + " has no enough volume capacity to create new volume: "
										+ volume_name + ", expected: " + volume_size + ", available: " + usable_size);
				}

				if (volume_size > t.size)
					return new RestfulReply(op, RetCode.INVALID_ARG,
							"tenant: " + tenant_name + " has no enough volume capacity to create new volume: " + volume_name
									+ ", expected: " + volume_size + ", available: " + usable_size);

			}

			v.id = S5Database.getInstance().queryLongValue("select gen_volume_id() as val") << 24;
			v.rep_count = Utils.getParamAsInt(request, "replica", 1);
			if(v.rep_count < 1 || v.rep_count >3)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG,
						String.format("rep_count:%d is invalid to create volume:%s. validate value shoule be 1,2,3", v.rep_count, volume_name));
			}
			v.name = volume_name;
			v.size = volume_size;
			v.shard_size = Config.DEFAULT_SHARD_SIZE;
			v.iops = Utils.getParamAsInt(request, "iops", 8 << 10);
			v.bw = Utils.getParamAsInt(request, "bw", 160 << 20);
			v.cbs = t.iops * 2;
			v.tenant_id = (int)t.id;
			v.status = Status.OK;
			tray_ids[0] = Utils.getParamAsString(request, "tray_0", null);
			tray_ids[1] = Utils.getParamAsString(request, "tray_1", null);
			tray_ids[2] = Utils.getParamAsString(request, "tray_2", null);
			store_name[0] = Utils.getParamAsString(request, "store_0", null);
			store_name[1] = Utils.getParamAsString(request, "store_1", null);
			store_name[2] = Utils.getParamAsString(request, "store_2", null);

		}
		catch (InvalidParamException | SQLException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}

		RestfulReply ret = select_store(op, v.rep_count, volume_size, store_name, tray_ids, store_idx);
		if (ret.retCode != 0)
		{
			return ret;
		}

		S5Database.getInstance().insert(v);
		long shardCount = (v.size + v.shard_size-1)/v.shard_size;
		for(int shardIndex = 0; shardIndex<shardCount;shardIndex++)
		{
			Shard shard = new Shard();
			shard.id=v.id | (shardIndex<<4);
			shard.primary_rep_index = 0;
			shard.volume_id = v.id;
			shard.status=Status.OK;
			shard.status_time = Timestamp.valueOf(LocalDateTime.now());
			S5Database.getInstance().insert(shard);
			for (int i = 0; i < v.rep_count; i++)
			{
				Replica r = new Replica();
				r.id=shard.id | i;
				r.volume_id = v.id;
				r.store_id = store_idx[i];
				r.tray_uuid = tray_ids[i];
				r.status = Status.OK;
				r.status_time = Timestamp.valueOf(LocalDateTime.now());
				S5Database.getInstance().insert(r);
			}
		}

		return new RestfulReply(op);
	}

	private RestfulReply select_store(String op, int replica_count, long volume_size, String[] store_names,
			String[] tray_ids, int[] store_ids)
	{
		if (replica_count != 1 && replica_count != 3)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, "invalid replica count");
		}

		boolean store_specified = false;
		boolean tray_specified = false;
		for (int i = 0; i < replica_count; i++)
		{
			if (StringUtils.isEmpty(store_names[i]) && store_specified == true)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG,
						"replica count is" + replica_count + "but num." + i + "store is not specified");
			}

			if (store_names[i] != null)
				store_specified = true;

			if (tray_ids[i] == null && tray_specified == true)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG,
						"replica count is" + replica_count + "but num." + i + "store is not specified");
			}

			if (tray_ids[i] != null)
				tray_specified = true;
		}

		if (!store_specified && tray_specified)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG,
					"if user choose to specify stores when create volume, trays must be also specified");
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
				return new RestfulReply(op, RetCode.INVALID_ARG, "replica count is not equal to store count");
			}

			for (int i = 0; i < replica_count; i++)
			{
				StoreNode s = S5Database.getInstance().table("t_store")
						.where("name=? AND status=?", store_names[i], StoreNode.STATUS_OK).first(StoreNode.class);
				store_ids[i] = s.id;
			}

			if (tray_specified)
			{
				for (int i = 0; i < replica_count; i++)
				{
					S5Database.getInstance()
							.sql("select t_tray.id from t_tray, v_tray_free_size where name=? and t_tray.store_id=? and "
									+ " t_tray.status=0 and v_tray_free_size.free_size>=? and v_tray_free_size.store_id=t_tray.store_id "
									+ "and v_tray_free_size.tray_uuid=t_tray.uuid ",
							"TRAY-" + tray_ids[i], store_ids[i], volume_size).first(Integer.class);
				}
				return new RestfulReply(op);
			}
		}

		RestfulReply ret = select_suitable_store_tray(op, replica_count, volume_size, store_names, store_ids, tray_ids);
		try
		{
			int r = (int) ret.retCode;
			if (r != 0)
				return ret;
		}
		catch (Exception e)
		{

		}
		return new RestfulReply(op);
	}

	private RestfulReply select_suitable_store_tray(String op, int replica_count, long volume_size,
			String[] store_names, int[] store_ids, String[] tray_ids)
	{
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
							+ "where t.store_id = s.store_id and t.free_size>=? and t.status=0 "
							+ "group by t.store_id order by s.free_size desc limit 3 ", volume_size)
					.results(HashMap.class);

			if (list.size() < replica_count)
				return new RestfulReply(op, RetCode.INVALID_ARG, "only" + list.size()
						+ "stores has tray with capacity over" + volume_size + "but replica is" + replica_count);

			// now choose tray for replica^M

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
						.sql("select store_id, tray_uuid, max(free_size),0 from v_tray_free_size where free_size >=? and store_id=? and status=0",
								volume_size, store_ids[i])
						.results(HashMap.class);

				if (list.size() < 1)
					return new RestfulReply(op, RetCode.INVALID_ARG,
							"store user specified for replica" + i + "has no tray with capacity over" + volume_size);

				for (HashMap h : list)
					tray_ids[i] = (String) h.get("tray_uuid");
			}
		}
		return null;
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
			tenant_name = Utils.getParamAsString(request, "tenant_name");
			Tenant tenant = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (tenant == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);

			volume_name = Utils.getParamAsString(request, "volume_name");
			Volume volume = S5Database.getInstance().table("t_volume").where("name=?", volume_name).first(Volume.class);
			if (volume == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume not exists: " + volume_name);

			Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			t_idx = (int)t.id;
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

			int bw = 0;
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
		String name = null;
		int limit = 0;
		try
		{
			name = Utils.getParamAsString(request, "by_tenant", "tenant_default");
			Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", name).first(Tenant.class);
			if (t == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + name);

			limit = Utils.getParamAsInt(request, "limit", 20);
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		int tenant_idx = S5Database.getInstance().sql("select id from t_tenant where name=?", name)
				.first(Integer.class);
		List<Volume> volumes = S5Database.getInstance()
				.sql("select id, name, size, iops, bw from t_volume where tenant_id=?", tenant_idx).results(Volume.class);

		if (limit >= volumes.size())
			limit = volumes.size();


		RestfulReply reply = new ListVolumeReply(request.getParameter("op"), volumes);
		return reply;

	}

	PrepareVolumeArg getPrepareArgs(String tenant_name, String volume_name) throws InvalidParamException {
		Volume vol = S5Database.getInstance()
				.sql("select t_volume.* from t_volume, t_tenant where t_tenant.name=? and t_volume.name=? and t_volume.tenant_id=t_tenant.id",
						tenant_name, volume_name)
				.first(Volume.class);
		if (vol == null)
			throw new InvalidParamException("Volume:" + tenant_name + ":" + volume_name + " not exists");
		if (!vol.status.equals(Status.OK))
			throw new InvalidParamException("Volume:" + tenant_name + ":" + volume_name + " in status (" + vol.status + ") can't be exposed");


		List<Replica> reps = S5Database.getInstance().where("volume_id=? and status!=?", vol.id, Status.ERROR)
				.orderBy("status asc").results(Replica.class);
		if (reps.size() == 0)
			throw new InvalidParamException("Volume:" + tenant_name + ":" + volume_name + " no replicas available");

		PrepareVolumeArg arg = new PrepareVolumeArg();
		arg.status = vol.status;
		arg.volume_name = vol.name;
		arg.volume_size = vol.size;
		arg.volume_id =vol.id;
		arg.shard_count = 1;//TODO: not support shard yet
		arg.rep_count=vol.rep_count;

		arg.shards = new ArrayList<>(arg.shard_count);
		for(int i=0;i<arg.shard_count;i++)
		{
			ShardArg shard = new ShardArg();
			shard.index=i;
			shard.replicas = new ArrayList<>(arg.rep_count);
			for(int repIdx = 0; repIdx<arg.rep_count;repIdx ++)
			{
				ReplicaArg rep = new ReplicaArg();
				rep.index = repIdx;
				rep.tray_uuid = reps.get(repIdx).tray_uuid;
				rep.store_id = reps.get(repIdx).store_id;
				shard.replicas.add(rep);
			}
			arg.shards.add(shard);
		}
		return arg;
	}
	public RestfulReply prepareVolumeOnStore(StoreNode s, VolumeHandler.PrepareVolumeArg arg) throws IOException, InterruptedException, TimeoutException, ExecutionException {
		GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
		Gson gson = builder.create();
		String jsonStr = gson.toJson(arg);

		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		ContentResponse response = client.newRequest(String.format("http://%s:49181/api?op=prepare_volume&name=?",
				s.mngtIp, URLEncoder.encode(arg.volume_name, StandardCharsets.UTF_8.toString())))
				.method(org.eclipse.jetty.http.HttpMethod.POST)
				.content(new org.eclipse.jetty.client.util.StringContentProvider(jsonStr), "application/json")
				.send();


		return gson.fromJson(new String(response.getContent()), RestfulReply.class);
	}

	private void markReplicasOnStoreAsError(int storeId, long volumeId)
	{
		S5Database.getInstance().sql("update t_replica set status='ERROR' where volume_id=? and store_id=?", volumeId, storeId);
	}

	private OpenVolumeReply getVolumeInfoForClient(long volumeId)
	{
		Volume v = S5Database.getInstance().where("id=?", volumeId).first(Volume.class);
		List<Port> ports = S5Database.getInstance().sql("select t_port.* from t_port, t_store where t_port.purpose=? and port.store_id=store.id and (store.status=? or store.status=?)",
				Port.NORMAL, Status.OK, Status.MAINTAIN).results(Port.class);
		HashMap<Integer, ArrayList<String>> portMap = new HashMap<>();
		for(Iterator<Port> it=ports.iterator();it.hasNext();)
		{
			Port p = it.next();
			if(portMap.containsKey(p.storeId))
				portMap.get(p.storeId).add(p.ipv4);
			else {
				ArrayList<String> ips = new ArrayList<>(4);
				ips.add(p.ipv4);
				portMap.put(p.storeId, ips);
			}
		}

		class TempShard
		{
			public long storeId;
			public int isPrimary;
			public int shardIndex;
		}

		List<TempShard> rawShards = S5Database.getInstance().sql("select store_id, is_primary, shard_index from v_replica_ext as r\n" +
				" where  volume_id=? and r.status='OK' order by shard_index, is_primary desc, status_time asc, replica_index asc", volumeId).results(TempShard.class);
		HashMap<Integer, ShardInfoForClient> shards = new HashMap<>(rawShards.size());
		for(Iterator<TempShard> it=rawShards.iterator(); it.hasNext(); )
		{
			TempShard s = it.next();
			ShardInfoForClient shard = shards.get(s.shardIndex);
			if(shard == null)
			{
				shard = new ShardInfoForClient();
				shard.index = s.shardIndex;
				shard.store_ips = (ArrayList<String>)portMap.get(s.storeId).clone();
				shards.put(s.shardIndex, shard);
			}
			else
			{
				shard.store_ips.addAll(portMap.get(s.storeId));
			}

		}

		OpenVolumeReply reply = new OpenVolumeReply("open_volume", RetCode.OK, null);
		reply.op="open_volume";
		reply.volume_id=v.id;
		reply.volume_name=v.name;
		reply.rep_count=v.rep_count;
		reply.volume_size=v.size;
		reply.status = v.status;
		reply.shards = new ShardInfoForClient[shards.values().size()];
		shards.values().toArray(reply.shards);
		return reply;
	}
	public RestfulReply open_volume(HttpServletRequest request, HttpServletResponse response) {
		String op = request.getParameter("op");
		String volume_name;
		String tenant_name;
		long volumeId;
		try {
			volume_name = Utils.getParamAsString(request, "volume_name");
			tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			boolean need_reprepare;
			do {
				need_reprepare = false;
				PrepareVolumeArg arg = getPrepareArgs(tenant_name, volume_name);
				volumeId = arg.volume_id;
				if (arg.status == Status.ERROR) {
					return new RestfulReply(op, RetCode.INVALID_STATE, String.format("Failed to open volume:%s for it's in ERROR state", volume_name));
				}
				List<StoreNode> stores = S5Database.getInstance()
						.sql("select t_store.* from t_store where id in (select distinct store_id from t_replica where volume_id=? )",
								arg.volume_id).results(StoreNode.class);

				for (Iterator<StoreNode> it = stores.iterator(); it.hasNext(); ) {
					StoreNode s = it.next();
					try {
						RestfulReply rply = prepareVolumeOnStore(s, arg);
						if (rply.retCode != RetCode.OK) {
							logger.error("Failed to prepare volume %{} on store:%{}, for:{}", volume_name, s.mngtIp, rply.reason);
							return rply;
						}
					} catch (IOException | InterruptedException | TimeoutException | ExecutionException e) {
						logger.error("Failed to prepare volume %{} on store:%{}, for:{}", volume_name, s.mngtIp, e);
						markReplicasOnStoreAsError(s.id, arg.volume_id);
						need_reprepare = true;
						break;
					}
				}
			} while (need_reprepare);
		} catch (InvalidParamException e1) {
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());
		}
		return getVolumeInfoForClient(volumeId);
	}

}
