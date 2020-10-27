package com.netbric.s5.orm;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_tray")
public class Tray
{
	@Id
	public String uuid;
	public String device;// varchar(96),
	public String status; // value can be on of STATE_XXX
	public long raw_capacity;
	public int store_id;
	public long object_size;

	public Tray()
	{
	}

}
