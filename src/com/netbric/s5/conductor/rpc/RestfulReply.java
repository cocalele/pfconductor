package com.netbric.s5.conductor.rpc;


public class RestfulReply
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    public String reason;
    public String op;
	public int retCode;//code defined in interface RetCode

	@SuppressWarnings("unchecked")
	public RestfulReply(String op)
	{
	    this.op= op+"_reply";
	}

	@SuppressWarnings("unchecked")
	public RestfulReply(String op, int retCode, String reason)
	{
        this.op= op+"_reply";
        this.retCode = retCode;
        this.reason = reason;
	}

	public void setRetCode(int retCode)
	{
		this.retCode = retCode;
	}

    public void setFail(int code, String reason) {
	    this.retCode = code;
	    this.reason = reason;
    }
}
