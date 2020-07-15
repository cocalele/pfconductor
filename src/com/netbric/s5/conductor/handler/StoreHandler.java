package com.netbric.s5.conductor.handler;

import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netbric.s5.conductor.rpc.ListDiskReply;
import com.netbric.s5.conductor.rpc.ListStoreReply;
import com.netbric.s5.orm.Port;
import org.apache.commons.lang3.StringUtils;

import com.netbric.s5.conductor.InvalidParamException;
import com.netbric.s5.conductor.RestfulReply;
import com.netbric.s5.conductor.RetCode;
import com.netbric.s5.conductor.SshExec;
import com.netbric.s5.conductor.Utils;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.StoreNode;
import com.netbric.s5.orm.Tray;
import com.netbric.s5.orm.Status;

/**
 * handle all request related with store node, include - add - delete - query
 * 
 * @author liulele
 *
 */
public class StoreHandler
{

	/**
	 * backend handler of CLI s5_add_store_node.py
	 * 
	 * @param request
	 * @param response
	 * @return
	 */
	public RestfulReply add_storenode(HttpServletRequest request, HttpServletResponse response)
	{
		StoreNode n = new StoreNode();
		String op = request.getParameter("op");
		try
		{
			n.name = Utils.getParamAsString(request, "device");
			StoreNode s = S5Database.getInstance().table("t_store").where("name=?", n.name).first(StoreNode.class);
			if (s != null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "store node already exists:" + n.name);

			n.mngtIp = Utils.getParamAsString(request, "mngt_ip");
			s = S5Database.getInstance().table("t_store").where("mngt_ip=?", n.mngtIp).first(StoreNode.class);
			if (s != null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "store node ip already exists:" + n.mngtIp);

			n.model = " ";
			n.sn = n.name + "-0000AB00";
			n.status = StoreNode.STATUS_OK;

		}
		catch (InvalidParamException e)
		{
			return new RestfulReply(op, RetCode.INVALID_ARG, e.getMessage());
		}

		S5Database.getInstance().insert(n);

		Tray t = new Tray();
		for (int i = 0; i < 20; ++i)
		{

			t.device = "Tray-" + i;
			t.status = Status.OK;
			t.raw_capacity = 8L << 40;
			t.store_id = n.id;
			S5Database.getInstance().insert(t);
		}

		return new RestfulReply(op);
	}

	public RestfulReply delete_storenode(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String name = null;
		StoreNode s = null;
		try
		{
			name = Utils.getParamAsString(request, "device");
			s = S5Database.getInstance().table("t_store").where("name=?", name).first(StoreNode.class);
			if (s == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "store node not exists:" + name);
		}
		catch (InvalidParamException e)
		{
			e.printStackTrace();
		}
		Integer count = S5Database.getInstance()
				.sql("select count(*) from t_replica as r,t_store as t where r.store_id=t.id and t.name=?", name)
				.first(Integer.class);

		if (count > 0)
		{
			return new RestfulReply(op, RetCode.INVALID_STATE,
					"Deleted store error, as there are replicas under the store!");
		}
		else
		{
			S5Database.getInstance().table("t_store").where("name=?", name).delete();

			S5Database.getInstance().sql("delete from t_tray where store_id=?", s.id).execute();

			return new RestfulReply(op);
		}
	}

	public RestfulReply list_storenode(HttpServletRequest request, HttpServletResponse response)
	{
		List<StoreNode> nodes = S5Database.getInstance().results(StoreNode.class);

		RestfulReply reply = new ListStoreReply(request.getParameter("op"), nodes);
		return reply;
	}
	public RestfulReply list_tray(HttpServletRequest request, HttpServletResponse response)
	{
		List<Tray> trays = S5Database.getInstance().results(Tray.class);

		RestfulReply reply = new ListDiskReply(request.getParameter("op"), trays);
		return reply;
	}

	public static class SanityCheckReply extends RestfulReply
	{
		public SanityCheckReply(String op, int retCode, HashMap<String, String> rst)
		{
			super(op);
			results = rst;
		}
		public HashMap<String, String> results;
	}
	public RestfulReply sanity_check(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String hostname = request.getParameter("hostname");
		if (StringUtils.isEmpty(hostname))
			return new RestfulReply(op, RetCode.INVALID_ARG, "Invalid argument: hostname");
		try
		{
			StoreNode node = S5Database.getInstance().where("name=?", hostname).first(StoreNode.class);
			if (node == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "No such store node:" + hostname);
			SshExec executer = new SshExec(node.mngtIp);
			boolean allok = true;
			HashMap<String, String> r = new HashMap<>();
			if (0 != executer.execute("echo Hello"))
			{
				r.put("ssh", "FAILED:" + executer.getStdout());
				allok = false;
			}
			else
				r.put("ssh", "OK");
			if (0 != executer.execute("pidof lt-raio_server"))
			{
				r.put("raio_server", "FAILED: raio_server not running");
				allok = false;
			}
			else
				r.put("raio_server", "OK");

			if (0 != executer.execute("pidof s5afs"))
			{
				r.put("s5afs", "FAILED: s5afs not running");
				allok = false;
			}
			else
				r.put("s5afs", "OK");

			if (0 != executer.execute("pidof bdd"))
			{
				r.put("bdd", "FAILED: bdd not running");
				allok = false;
			}
			else
				r.put("bdd", "OK");

			if (0 != executer.execute("which nbdxadm"))
			{
				r.put("nbdxadm", "FAILED: nbdxadm can not found");
				allok = false;
			}
			else
				r.put("nbdxadm", "OK");
			if (0 != executer.execute("lsmod | grep nbdx"))
			{
				r.put("nbdx.ko", "FAILED: nbdx.ko not installed");
				allok = false;
			}
			else
				r.put("nbdx.ko", "OK");

			if (0 != executer.execute("lsmod | grep s5bd"))
			{
				r.put("s5bd.ko", "FAILED: s5bd.ko not installed");
				allok = false;
			}
			else
				r.put("s5bd.ko", "OK");

			if (0 != executer.execute("which s5bd"))
			{
				r.put("s5bd", "FAILED: s5bd command not found");
				allok = false;
			}
			else
				r.put("s5bd", "OK");
			RestfulReply reply = new SanityCheckReply(op,RetCode.OK, r);
			if (!allok)
				reply.setRetCode(RetCode.REMOTE_ERROR);
			return reply;
		}
		catch (Exception ex)
		{
			return new RestfulReply(op, RetCode.DB_ERROR, ex.getMessage());
		}
	}

	public static class ListNodePortReply extends RestfulReply
    {
        public Port[] ports;
        public ListNodePortReply(String op)
        {
            super(op);
        }
    }
	public RestfulReply list_nodeport(HttpServletRequest request, HttpServletResponse response)
	{
		String op = request.getParameter("op");
		String hostname = request.getParameter("node_name");
		if (StringUtils.isEmpty(hostname))
			return new RestfulReply(op, RetCode.INVALID_ARG, "Invalid argument: node_name");
		try
		{
			StoreNode node = S5Database.getInstance().where("name=?", hostname).first(StoreNode.class);
			if (node == null)
				return new RestfulReply(op, RetCode.INVALID_ARG, "No such store node:" + hostname);
			SshExec executer = new SshExec(node.mngtIp);
			RestfulReply r = new RestfulReply(op);
			boolean allok = true;
			String[] ethArray = null;
			if (0 != executer.execute("echo `ip -4 -o addr|grep 'eth'|awk '{print $2}'`"))
			{
			    r.setFail(-1, executer.getStdout());
				allok = false;
			}
			else
			{
				String result1 = executer.getStdout();
				result1 = result1.replace("\n", "");
				ethArray = result1.split(" ");
			}
			if (0 != executer.execute("echo `ip -4 -o addr|grep 'eth'|awk '{print $4}'|awk -F/ '{print $1}'`"))
			{
                r.setFail(-1, executer.getStdout());
				allok = false;
			}
			else
			{
				String result = executer.getStdout();
				result = result.replace("\n", "");
				String[] ipArray = result.split(" ");
                ListNodePortReply rl = new ListNodePortReply(op);
				rl.ports=  new Port[ipArray.length];
				for (int i = 0; i < ipArray.length; i++)
				{
					String ip = ipArray[i];
					HashMap<String, String> map = new HashMap<String, String>();
					String portName = ethArray[i];
					if (!portName.contains("eth"))
					{
						continue;
					}
                    rl.ports[i] = new Port();
					rl.ports[i].ip_addr = ip;
					rl.ports[i].name = ethArray[i];
				}
				r=rl;
			}
			if (!allok)
				r.setRetCode(RetCode.REMOTE_ERROR);
			return r;
		}
		catch (Exception ex)
		{
			return new RestfulReply(op, RetCode.DB_ERROR, ex.getMessage());
		}

	}
}
