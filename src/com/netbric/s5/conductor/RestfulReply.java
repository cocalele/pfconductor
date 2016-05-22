package com.netbric.s5.conductor;

import org.json.simple.JSONObject;

public class RestfulReply extends JSONObject
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public RestfulReply(String op)
	{
		this.put("op", op + "_reply");
		this.put("ret_code", 0);
	}

	@SuppressWarnings("unchecked")
	public RestfulReply(String op, int retCode, String reason)
	{
		this.put("op", op + "_reply");
		this.put("ret_code", retCode);
		this.put("reason", reason);
	}

	public void setRetCode(int retCode)
	{
		this.put("ret_code", retCode);
	}

}
