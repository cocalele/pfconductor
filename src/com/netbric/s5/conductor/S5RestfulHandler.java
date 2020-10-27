package com.netbric.s5.conductor;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.conductor.handler.StoreHandler;
import com.netbric.s5.conductor.handler.TenantHandler;
import com.netbric.s5.conductor.handler.VolumeHandler;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.RetCode;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.Volume;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


public class S5RestfulHandler extends AbstractHandler
{
	static final Logger logger = LoggerFactory.getLogger(S5RestfulHandler.class);
	VolumeHandler volumeHandler = new VolumeHandler();
	TenantHandler tenantHandler = new TenantHandler();
	StoreHandler storenodeHandler = new StoreHandler();
	SnapshotManager snapshotHandler = new SnapshotManager();


	private RestfulReply unexport_volume(HttpServletRequest request, HttpServletResponse response) throws IOException
	{
		String op = request.getParameter("op");
		String volume_name = request.getParameter("volume_name");
		if (StringUtils.isEmpty(volume_name))
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, "Invalid argument: volume_name");
		}
		String tenant_name = request.getParameter("tenant_name");
		if (StringUtils.isEmpty(tenant_name))
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, "Invalid argument: tenant_name");
		}

		Volume vol = S5Database.getInstance().where("volume_name=? AND tenant_name=?", volume_name, tenant_name)
				.first(Volume.class);
		if (vol == null)
			return new RestfulReply(op, RetCode.INVALID_ARG, "Volume not found");
		StringBuffer sb = new StringBuffer();

		// List<StoreNode> nodes = S5Database.getInstance().where("hostname in
		// ('"
		// +StringUtils.replace(vol.node, ",", "','")
		// +"')"
		// ).results(StoreNode.class);
		// for(StoreNode node : nodes)
		// {
		// SshExec executer = new SshExec(node.mngtIp);
		// if(0 != executer.execute("tgtadm --lld iscsi --mode target --op
		// delete --tid "+vol.idx ))
		// {
		// sb.append( "Node:"+node.hostname+":"+executer.getStdout());
		// continue;
		// }
		// if(0 != executer.execute("sed -i '/"+vol.iqn+">/,/<\\/target>/s/^/#/'
		// /etc/tgt/targets.conf" ))
		// {
		// sb.append( "Node:"+node.hostname+":"+executer.getStdout());
		// continue;
		// }
		// if(0 != executer.execute("s5bd unmap -t " + tenant_name +" -v
		// "+volume_name ))
		// {
		// sb.append( "Node:"+node.hostname+":"+executer.getStdout());
		// continue;
		// }
		//
		//
		// }
		S5Database.getInstance().delete(vol);
		if (sb.length() == 0)
			return new RestfulReply(op);
		else
			return new RestfulReply(op, RetCode.REMOTE_ERROR, sb.toString());

	}

	public synchronized void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		response.setContentType("text/json; charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		String op = request.getParameter("op");
		//String fmt = Utils.getParamAsString(request, "fmt", "json");
		logger.debug("API called: op={}", op);
		RestfulReply reply;
		try
		{
			if ("add_store".equals(op))
				reply = storenodeHandler.add_storenode(request, response);
			else if ("delete_store".equals(op))
				reply = storenodeHandler.delete_storenode(request, response);
			else if ("node_sanity_check".equals(op))
				reply = storenodeHandler.sanity_check(request, response);
			else if ("list_store".equals(op))
				reply = storenodeHandler.list_storenode(request, response);
			else if ("list_port".equals(op))
				reply = storenodeHandler.list_nodeport(request, response);
			else if ("list_disk".equals(op))
				reply = storenodeHandler.list_tray(request, response);
			else if ("create_tenant".equals(op))
				reply = tenantHandler.createTenant(request, response);
			else if ("update_tenant".equals(op))
				reply = tenantHandler.updateTenant(request, response);
			else if ("delete_tenant".equals(op))
				reply = tenantHandler.deleteTenant(request, response);
			else if ("list_tenant".equals(op))
				reply = tenantHandler.listTenant(request, response);
			else if ("create_volume".equals(op))
				reply = volumeHandler.create_volume(request, response);
			else if ("update_volume".equals(op))
				reply = volumeHandler.update_volume(request, response);
			else if ("delete_volume".equals(op))
				reply = volumeHandler.delete_volume(request, response);
			else if ("list_volume".equals(op))
				reply = volumeHandler.list_volume(request, response);
			else if ("expose_volume".equals(op))
				reply = volumeHandler.expose_volume(request, response);
			else if ("unexpose_volume".equals(op))
				reply = volumeHandler.unexpose_volume(request, response);
			else if ("open_volume".equals(op))
				reply = volumeHandler.open_volume(request, response);
			else if("create_snapshot".equals(op))
				reply = volumeHandler.createSnapshot(request, response);
			else if("recovery_volume".equals(op))
				reply = volumeHandler.recoveryVolume(request, response);
			else if("query_task".equals(op))
				reply = volumeHandler.queryTask(request, response);
			else
			{
				reply = new RestfulReply(op, RetCode.INVALID_OP, "Invalid op:" + op);
			}
		}
		catch (Exception ex)
		{
			reply = new RestfulReply(op, RetCode.INVALID_OP, ex.getMessage());
			logger.error("Error processing:" + op, ex);
		}
        GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
		        .setPrettyPrinting();
		       // .serializeNulls();
        Gson gson = builder.create();
        String reply_str = gson.toJson(reply);
        logger.info("{}", reply_str);
        response.getWriter().write(reply_str);
        baseRequest.setHandled(true);
	}

}
