package com.netbric.s5.conductor.rpc;

import com.netbric.s5.conductor.BackgroundTaskManager;

public class StoreQueryTaskReply extends RestfulReply {
	public long taskId;
	public BackgroundTaskManager.TaskStatus status;


	public StoreQueryTaskReply(){
		super("");
	}
	public StoreQueryTaskReply(String op, int retCode, String reason) {
		super(op, retCode, reason);

	}
}