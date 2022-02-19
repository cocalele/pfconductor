package com.netbric.s5.conductor.handler;

import com.netbric.s5.conductor.Utils;
import com.netbric.s5.conductor.exception.InvalidParamException;
import com.netbric.s5.conductor.rpc.ListAofReply;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.RetCode;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Tenant;
import com.netbric.s5.orm.Volume;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;

public class AofHandler {
	public static RestfulReply ls_children(HttpServletRequest request, HttpServletResponse response){
		String op = request.getParameter("op");

		String tenant_name;
		try
		{
			tenant_name = Utils.getParamAsString(request, "tenant_name");
			Tenant t = S5Database.getInstance().table("t_tenant").where("name=?", tenant_name).first(Tenant.class);
			if (t == null) {
				return new RestfulReply(null, RetCode.INVALID_ARG, "tenant not exists: " + tenant_name);
			}
			List<Volume> vols = S5Database.getInstance().table("t_volume").where("name like ?", t.name+"/").results(Volume.class);
			ArrayList<String> names = new ArrayList<>(vols.size());
			for(Volume v : vols){
				names.add(v.name);
			}
			return new ListAofReply(op, names);
		}
		catch (InvalidParamException e1)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e1.getMessage());
		}
	}
}
