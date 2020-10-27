package com.netbric.s5.conductor.rpc;

import com.netbric.s5.conductor.BackgroundTaskManager;

public class BackgroundTaskReply extends RestfulReply {
	public long taskId;
	public BackgroundTaskManager.BackgroundTask task;
	public BackgroundTaskReply(String op, BackgroundTaskManager.BackgroundTask task) {
		super(op);
		this.taskId = task.id;
		this.task = task;
	}

	public BackgroundTaskReply(String op, int retCode, String reason) {
		super(op, retCode, reason);
	}
}
