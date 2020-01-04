package com.netbric.s5.orm;

import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Table(name = "t_shard")
public class Shard {
    @Id
    public long id;
    public long volume_id;
    public int shard_index;
    public int primary_rep_index;
    public String status;
    public Timestamp status_time;
}
