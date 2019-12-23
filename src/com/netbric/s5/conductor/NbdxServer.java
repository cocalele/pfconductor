package com.netbric.s5.conductor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netbric.s5.orm.Replica;
import com.netbric.s5.orm.S5Database;
import com.netbric.s5.orm.StoreNode;
import com.netbric.s5.orm.Volume;

public class NbdxServer
{

	static final Logger logger = LoggerFactory.getLogger(NbdxServer.class);

	/**
	 * expose volume's all replica on correspond node, the first replica in
	 * reps_ must be primary
	 * 
	 * @param vol
	 * @param reps_
	 * @throws IOException
	 * @return an array of mount point, can be used by nbdx mount
	 */
	public static MountTarget[] exposeVolume(Volume vol, List<Replica> reps_) throws OperateException, IOException
	{
		Replica[] reps = reps_.toArray(new Replica[reps_.size()]);
		StringBuilder sb = new StringBuilder(reps[0].store_id + "");
		for (int i = 1; i < reps.length; i++)
			sb.append("," + reps[i].store_id);

		List<StoreNode> nodes = S5Database.getInstance().where("id in (" + sb.toString() + ")")
				.results(StoreNode.class);
		SshExec[] sshs = new SshExec[nodes.size()];
		String[] devNames = new String[nodes.size()];
		for (int i = 0; i < reps.length; i++)
		{
			StoreNode n = nodes.get(i);
			sshs[i] = new SshExec(n.mngtIp);

			String devSuffix = "s5r_" + vol.tenant_id + "_" + vol.name;
			devNames[i] = "/dev/" + devSuffix;
			// map --toe_ip <toe_ip> --toe_port <toe_port> --volume_id
			// <volume_id> --volume_size <volume_size(M)> [--dev_name
			// <dev_name>]
			if (0 != sshs[i].execute("s5bd map --toe_ip 127.0.0.1 --toe_port 0 --volume_id " + reps[i].id
					+ " --volume_size " + (vol.size >> 20) + " --dev_name " + devSuffix))
			{
				logger.error("Execute command on: {},{}, stdout:{}", sshs[i].targetIp, sshs[i].lastCli,
						sshs[i].getStdout());
				throw new OperateException("s5bd map failed on node:" + sshs[i].targetIp + "\n" + sshs[i].getStdout());
			}
		}

		// mount all replica to primary node via NBDX
		for (int i = 1; i < reps.length; i++)
		{
			if (0 != sshs[0].execute(
					"nbdxadm -o create_device -i " + reps[i].store_id + " -d " + reps[i].id + " -f " + devNames[i]))
			{
				logger.error("Execute command on: {},{}, stdout:{}", sshs[0].targetIp, sshs[0].lastCli,
						sshs[0].getStdout());
				throw new OperateException("nbdxadm failed on node:" + sshs[0].targetIp);
			}
			devNames[i] = "/dev/nbdx" + reps[i].id;
		}
		String primaryDevName = "/dev/md/s5v_" + vol.tenant_id + "_" + vol.name;
		if (0 != sshs[0]
				.execute("yes | mdadm -q --create " + primaryDevName + " --homehost= --level=1 --force --raid-devices="
						+ reps.length + " --spare-devices=0 " + StringUtils.join(devNames, ' ')))
		{
			logger.error("Execute command on: {},{}, stdout:{}", sshs[0].targetIp, sshs[0].lastCli,
					sshs[0].getStdout());
			throw new OperateException("mdadm failed on node:" + sshs[0].targetIp);
		}
		if (0 != sshs[0].execute("mdadm --examine --scan | grep " + primaryDevName))
		{
			logger.error("Execute command on: {},{}, stdout:{}", sshs[0].targetIp, sshs[0].lastCli,
					sshs[0].getStdout());
			throw new OperateException("save config file on node:" + sshs[0].targetIp);
		}

		String mdmConfig = sshs[0].getStdout();

		for (int i = 0; i < reps.length; i++)
		{

			if (0 != sshs[i].execute("cat >> /etc/mdadm/mdadm.conf <<EOF_TGT\n" + mdmConfig + "\nEOF_TGT"))
			{
				logger.error("Execute command on: {},{}, stdout:{}", sshs[i].targetIp, sshs[i].lastCli,
						sshs[i].getStdout());
				throw new OperateException("save config file on node:" + sshs[i].targetIp);
			}
		}

		return new MountTarget[] { new MountTarget(sshs[0].targetIp, primaryDevName) };

		// persistent the configuration
		// StgtTargetConf cfg = new StgtTargetConf(vol.iqn, vol.idx,
		// devName);
		// if(0 != executer.execute("cat >> /etc/tgt/targets.conf
		// <<EOF_TGT\n" + cfg+"\nEOF_TGT"))
		// {
		// return new RestfulReply(op, RetCode.REMOTE_ERROR,
		// "Node:"+node.hostname+":"+executer.getStdout());
		// }

	}

	/**
	 * unexpose volume's all replica on correspond node, the first replica in
	 * reps_ must be primary
	 * 
	 * @param vol
	 * @param reps_
	 * @throws IOException
	 * @return an array of mount point, can be used by nbdx mount
	 */
	public static MountTarget[] unexposeVolume(Volume vol, List<Replica> reps_) throws OperateException, IOException
	{
		Replica[] reps = reps_.toArray(new Replica[reps_.size()]);
		StringBuilder sb = new StringBuilder(reps[0].store_id + "");
		for (int i = 1; i < reps.length; i++)
			sb.append("," + reps[i].store_id);
	
		List<StoreNode> nodes = S5Database.getInstance().where("id in (" + sb.toString() + ")")
				.results(StoreNode.class);
		SshExec[] sshs = new SshExec[nodes.size()];
		String primaryDevName = "/dev/md/s5v_" + vol.tenant_id + "_" + vol.name;
	
		for (int i = 0; i < reps.length; i++)
		{
			StoreNode n = nodes.get(i);
			sshs[i] = new SshExec(n.mngtIp);
			if(0 != sshs[i].execute("sed -i '/"+primaryDevName+">/,/<\\/target>/s/^/#/' /etc/mdadm/mdadm.conf" ))
			{
				logger.error("Execute command on: {},{}, stdout:{}", sshs[i].targetIp, sshs[i].lastCli,
						sshs[i].getStdout());
			}
			if (0 != sshs[i].execute("mdadm --stop " + primaryDevName))
			{
				logger.error("Execute command on: {},{}, stdout:{}", sshs[i].targetIp, sshs[i].lastCli,
						sshs[i].getStdout());
			}
			String devSuffix = "s5r_" + vol.tenant_id + "_" + vol.name;
			if (0 != sshs[i].execute("s5bd unmap -n " + devSuffix))
			{
				logger.error("Execute command on: {},{}, stdout:{}", sshs[i].targetIp, sshs[i].lastCli,
						sshs[i].getStdout());
			}
		}
	
		return new MountTarget[] { new MountTarget(sshs[0].targetIp, primaryDevName) };	
	}
}
