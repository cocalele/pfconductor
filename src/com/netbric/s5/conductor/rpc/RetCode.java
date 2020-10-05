package com.netbric.s5.conductor.rpc;

public interface RetCode
{
	public static int OK = 0;
	public static int INVALID_OP = 1;
	public static int INVALID_ARG = 2;
	public static int DUPLICATED_NODE = 3;
	public static int DB_ERROR = 4;
	public static int REMOTE_ERROR = 5;
	public static int ALREADY_DONE = 6;
	public static int INVALID_STATE = 7;
}
