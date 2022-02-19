package com.netbric.s5.conductor.rpc;


import java.util.List;

public class ListAofReply extends RestfulReply {
	public ListAofReply(String op, List<String> files)
	{
		super(op);
		this.files = files;
	}
	public List<String> files;
}
