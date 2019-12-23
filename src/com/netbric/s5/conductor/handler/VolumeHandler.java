package com.netbric.s5.conductor.handler;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netbric.s5.orm.*;
import org.apache.commons.lang3.StringUtils;

import com.netbric.s5.conductor.InvalidParamException;
import com.netbric.s5.conductor.MountTarget;
import com.netbric.s5.conductor.NbdxServer;
import com.netbric.s5.conductor.OperateException;
import com.netbric.s5.conductor.RestfulReply;
import com.netbric.s5.conductor.RetCode;
import com.netbric.s5.conductor.Utils;

import static com.netbric.s5.orm.S5Database.*;

public class VolumeHandler
{
	public static class ExposeVolumeReply extends RestfulReply
	{
		public ExposeVolumeReply(String op, MountTarget[] tgts){
			super(op);
			targets = tgts;
		}
		MountTarget[] targets;
	}
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
		Volume vol = getInstance()
				.sql("select t_volume.* from t_volume, t_tenant where t_tenant.device=? and t_volume.device=? and t_volume.tenant_id=t_tenant.id",
						tenant_name, volume_name)
				.first(Volume.class);
		if (vol == null)
			return new RestfulReply(op, RetCode.INVALID_ARG,
					"Volume:" + tenant_name + ":" + volume_name + " not exists");
		if (!vol.status.equals(Status.OK))
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Volume:" + tenant_name + ":" + volume_name + " in status (" + vol.status + ") can't be exposed");
		if (vol.exposed == true)
			return new RestfulReply(op, RetCode.ALREADY_DONE,
					"Volume:" + tenant_name + ":" + volume_name + " has already been exposed");

		List<Replica> reps = getInstance().where("volume_id=? and status!=?", vol.id, Replica.STATUE_ERROR)
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
		Volume vol = getInstance()
				.sql("select t_volume.* from t_volume, t_tenant where t_tenant.device=? and t_volume.device=? and t_volume.tenant_id=t_tenant.id",
						tenant_name, volume_name)
				.first(Volume.class);
		if (vol == null)
			return new RestfulReply(op, RetCode.INVALID_ARG,
					"Volume:" + tenant_name + ":" + volume_name + " not exists");
		if (!vol.status.equals(Status.OK))
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Volume:" + tenant_name + ":" + volume_name + " in status (" + vol.status + ") can't be exposed");

		List<Replica> reps = getInstance().where("volume_id=? and status!=?", vol.id, Replica.STATUE_ERROR)
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
		int replica_count = 0;
		long volume_size = 0;
		long used_size = 0;
		long usable_size = 0;

		Volume v = new Volume();

		try
		{
			String tenant_name = Utils.getParamAsString(request, "tenant_name", "tenant_default");
			Tenant t = getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (t == null)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);
			}

			String volume_name = Utils.getParamAsString(request, "name");
			Volume volume = getInstance().table("t_volume")
					.where("name=? AND tenant_id=?", volume_name, t.id).first(Volume.class);
			if (volume != null)
			{
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume already exists: " + volume_name);
			}

			volume_size = Utils.getParamAsLong(request, "size", 4L << 30) ;

			if(t.size > 0)
			{
			int count = getInstance().sql("select count(*) from t_volume where tenant_id=?", t.id)
					.first(Long.class).intValue();

				if (count != 0)
				{
					HashMap m = getInstance().sql("select sum(size) as used from t_volume where tenant_id=?", t.id)
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

			v.id = getInstance().queryLongValue("select gen_volume_id() as val") << 24;
			replica_count = Utils.getParamAsInt(request, "replica", 1);
			v.name = volume_name;
			v.size = volume_size;
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

		RestfulReply ret = select_s5store(op, replica_count, volume_size, store_name, tray_ids, store_idx);
		try
		{
			int r = (int) ret.retCode;
			if (r != 0)
			{
				return ret;
			}
		}
		catch (Exception e)
		{

		}

		getInstance().insert(v);
		for (int i = 0; i < replica_count; i++)
		{
			Replica r = new Replica();
			r.id=v.id | i;
			r.volume_id = v.id;
			r.store_id = store_idx[i];
			r.tray_uuid = tray_ids[i];
			r.status = Status.OK;
			getInstance().insert(r);
		}

		return new RestfulReply(op);
	}

	private RestfulReply select_s5store(String op, int replica_count, long volume_size, String[] store_names,
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
				StoreNode s = getInstance().table("t_s5store")
						.where("device=? AND status=?", store_names[i], StoreNode.STATUS_OK).first(StoreNode.class);
				store_ids[i] = s.id;
			}

			if (tray_specified)
			{
				for (int i = 0; i < replica_count; i++)
				{
					getInstance()
							.sql("select t_tray.id from t_tray, v_tray_free_size where device=? and t_tray.store_id=? and "
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
			// 3. group by store_id, each s5store to get one record
			// 4. order by s.free_size desc, order s5 store by its free size
			List<HashMap> list = getInstance()
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
				List<HashMap> list = getInstance()
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
			Tenant tenant = getInstance().table("t_tenant").where("device=?", tenant_name).first(Tenant.class);
			if (tenant == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);

			volume_name = Utils.getParamAsString(request, "volume_name");
			Volume volume = getInstance().table("t_volume").where("device=?", volume_name).first(Volume.class);
			if (volume == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume not exists: " + volume_name);

			Tenant t = getInstance().table("t_tenant").where("device=?", tenant_name).first(Tenant.class);
			t_idx = (int)t.id;
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		int rowaffected = getInstance()
				.sql("delete from t_replica where t_replica.volume_id in (select t_volume.id from t_volume where device='"
						+ volume_name + "' and tenant_id=" + t_idx + ")")
				.execute().getRowsAffected();
		int r = getInstance().table("t_volume").where("device=? AND tenant_id=?", volume_name, t_idx).delete()
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
			Tenant t = getInstance().table("t_tenant").where("device=?", tenant_name).first(Tenant.class);
			if (t == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists:" + tenant_name);

			int idx = (int)t.id;

			volume_name = Utils.getParamAsString(request, "volume_name");
			Volume volume = getInstance().table("t_volume").where("device=?", volume_name).first(Volume.class);
			if (volume == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "volume not exists:" + volume_name);

			nv = getInstance().table("t_volume").where("device=? AND tenant_id=?", volume_name, idx)
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
		getInstance().update(nv);
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
			Tenant t = getInstance().table("t_tenant").where("name=?", name).first(Tenant.class);
			if (t == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists: " + name);

			limit = Utils.getParamAsInt(request, "limit", 20);
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		int tenant_idx = getInstance().sql("select id from t_tenant where name=?", name)
				.first(Integer.class);
		List<Volume> volumes = getInstance()
				.sql("select id, name, size, iops, bw from t_volume where tenant_id=?", tenant_idx).results(Volume.class);

		if (limit >= volumes.size())
			limit = volumes.size();


		RestfulReply reply = new ListVolumeReply(request.getParameter("op"), volumes);
		return reply;

	}

}
