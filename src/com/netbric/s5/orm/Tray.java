package com.netbric.s5.orm;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_tray")
public class Tray
{
	@Id
	@GeneratedValue
	public int id;
	public String name;// varchar(96),
	public int status; // value can be on of STATE_XXX
	public String model;
	public int bit;
	public int firmware;
	public long raw_capacity;
	public int store_id;

	public static final int STATUS_OK = 0;
	public static final int STATUS_NA = 1;
	public static final int STATUS_ERROR = 2;
	public static final int STATUS_WARNING = 3;
	public static final int STATUS_INITIALIZING = 4; // initializing
	public static final int STATUS_INVALID = 5;
	public static final int STATUS_IO_ERROR = 6;
	public static final int STATUS_MAX = 7;

	public Tray()
	{
	};

	public Tray(int idx, String name, int status, String model, int bit, int firmware, long raw_capacity, int store_idx)
	{
		super();
		this.id = idx;
		this.name = name;
		this.status = status;
		this.model = model;
		this.bit = bit;
		this.firmware = firmware;
		this.raw_capacity = raw_capacity;
		this.store_id = store_idx;
	}
}
