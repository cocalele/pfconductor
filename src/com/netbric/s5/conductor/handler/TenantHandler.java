package com.netbric.s5.conductor.handler;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netbric.s5.conductor.InvalidParamException;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.RetCode;
import com.netbric.s5.conductor.Utils;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Tenant;

public class TenantHandler
{

	public RestfulReply createTenant(HttpServletRequest request, HttpServletResponse response)
	{
		Tenant t = new Tenant();
		String op = request.getParameter("op");
		try
		{
			t.name = Utils.getParamAsString(request, "tenant_name");
			Tenant tenant = S5Database.getInstance().table("t_tenant").where("name=?", t.name).first(Tenant.class);
			if (tenant != null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant already exists:" + t.name);
			t.pass_wd = Utils.getParamAsString(request, "tenant_passwd", "123456");
			t.size = Utils.getParamAsLong(request, "size", 4194304) * 1024 * 1024;
			t.iops = Utils.getParamAsInt(request, "iops", 8) * 1024;
			t.bw = Utils.getParamAsInt(request, "bw", 160) * 1024 * 1024;
			t.cbs = t.iops * 2;
			t.auth = 0;
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		S5Database.getInstance().insert(t);
		return new RestfulReply(op);
	}

	public RestfulReply deleteTenant(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String name = null;
		try
		{
			name = Utils.getParamAsString(request, "tenant_name");
			Tenant tenant = S5Database.getInstance().table("t_tenant").where("name=?", name).first(Tenant.class);
			if (tenant == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists:" + name);
		}
		catch (InvalidParamException e)
		{
			e.printStackTrace();
		}
		Integer vols = S5Database.getInstance()
				.sql("select count(*) from t_volume as v,t_tenant as t where v.tenant_idx=t.idx and t.name=?", name)
				.first(Integer.class);
		if (vols > 0)
		{
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Deleted tenant error, as there are volumes under the tenant!");
		}
		else
		{
			S5Database.getInstance().table("t_tenant").where("name=?", name).delete();
			return new RestfulReply(op);
		}
	}

	public RestfulReply updateTenant(HttpServletRequest request, HttpServletResponse response)
	{
		Tenant nt = null;
		String tenant_name = null;

		String op = request.getParameter("op");
		try
		{
			tenant_name = Utils.getParamAsString(request, "tenant_name");
			nt = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (nt == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "tenant not exists:" + tenant_name);
			nt.name = Utils.getParamAsString(request, "new_tenant_name", nt.name);
			nt.pass_wd = Utils.getParamAsString(request, "new_tenant_passwd", nt.pass_wd);

			long size = 0;
			try
			{
				size = Utils.getParamAsLong(request, "size") * 1024 * 1024;
			}
			catch (Exception e)
			{
				size = nt.size;
			}
			nt.size = size;

			int iops = 0;
			try
			{
				iops = Utils.getParamAsInt(request, "iops") * 1024;
			}
			catch (Exception e)
			{
				iops = nt.iops;
			}
			nt.iops = iops;

			int bw = 0;
			try
			{
				bw = Utils.getParamAsInt(request, "bw") * 1024 * 1024;
			}
			catch (Exception e)
			{
				bw = nt.bw;
			}

			nt.bw = bw;
			nt.cbs = iops * 2;
			nt.auth = 0;
		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}
		S5Database.getInstance().update(nt);
		return new RestfulReply(op);
	}

	public static class ListTenantReply extends RestfulReply
	{
		public ListTenantReply(String op)
		{
			super(op);
		}
		public List<Tenant> tenants;
	}
	public RestfulReply listTenant(HttpServletRequest request, HttpServletResponse response)
	{
		List<Tenant> tenants = S5Database.getInstance().results(Tenant.class);
		ListTenantReply r = new ListTenantReply("list_tenant");
		r.tenants = tenants;

		return r;
	}

}
