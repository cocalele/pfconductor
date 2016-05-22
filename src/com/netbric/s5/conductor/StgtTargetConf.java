package com.netbric.s5.conductor;

/**
 * represent a STGT Target in the /etc/tgt/tgtd.conf A target have only one LUN
 * in this version. <target iqn.2015-12.com.netbric:S5:t1.v1> backing-store
 * /dev/s5v_t1v1 </target>
 * 
 * @author liulele
 *
 */
public class StgtTargetConf
{

	public StgtTargetConf(String iqn, int tid, String device)
	{
		super();
		this.iqn = iqn;
		this.tid = tid;
		this.device = device;
	}

	String iqn;
	int tid;
	String device;// backing store device

	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer("<target ").append(iqn).append(">\n"); // line
																					// 1
		sb.append("\tbacking-store ").append(device).append('\n'); // line 2
		sb.append("</target>\n"); // line 3
		return sb.toString();
	}
}
