package com.netbric.s5.orm;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "t_tray")
public class Tray
{
	@Id
	@GeneratedValue
	public int idx;
	public String name;// varchar(96),
	public int status; // value can be on of STATE_XXX
	public String model;
	public int bit;
	public int firmware;
	public long raw_capacity;
	public String set0_name;
	public int set0_status;
	public String set0_model;
	public int set0_bit;
	public String set1_name;
	public int set1_status;
	public String set1_model;
	public int set1_bit;
	public int store_idx;

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

	public Tray(int idx, String name, int status, String model, int bit, int firmware, long raw_capacity,
			String set0_name, int set0_status, String set0_model, int set0_bit, String set1_name, int set1_status,
			String set1_model, int set1_bit, int store_idx)
	{
		super();
		this.idx = idx;
		this.name = name;
		this.status = status;
		this.model = model;
		this.bit = bit;
		this.firmware = firmware;
		this.raw_capacity = raw_capacity;
		this.set0_name = set0_name;
		this.set0_status = set0_status;
		this.set0_model = set0_model;
		this.set0_bit = set0_bit;
		this.set0_name = set1_name;
		this.set0_status = set1_status;
		this.set0_model = set1_model;
		this.set0_bit = set1_bit;
		this.store_idx = store_idx;
	}
}
