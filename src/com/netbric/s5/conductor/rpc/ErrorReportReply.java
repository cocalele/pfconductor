package com.netbric.s5.conductor.rpc;

public class ErrorReportReply extends RestfulReply{
	public int meta_ver;
	public int action_code;


	public static final int ACTION_REOPEN = 0x4000;//must same value as MSG_STATUS_REOPEN

	public ErrorReportReply(String op, int action, int newMetaVer) {
		super(op);
		action_code = action;
		meta_ver = newMetaVer;
	}

	public ErrorReportReply(String op, int retCode, String reason) {
		super(op, retCode, reason);
	}
}
