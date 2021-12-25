package com.netbric.s5.conductor.rpc;

public interface RetCode
{
	public static final int OK = 0;
	public static final int INVALID_OP = 1;
	public static final int INVALID_ARG = 2;
	public static final int DUPLICATED_NODE = 3;
	public static final int DB_ERROR = 4;
	public static final int REMOTE_ERROR = 5;
	public static final int ALREADY_DONE = 6;
	public static final int INVALID_STATE = 7;
	public static final int VOLUME_NOT_EXISTS = 8;
}
