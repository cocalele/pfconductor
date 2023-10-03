package com.netbric.s5.orm;

import javax.persistence.Column;
import javax.persistence.Table;
import javax.persistence.Id;
import javax.persistence.Transient;
import java.util.HashMap;

@Table(name = "t_shared_disk")
public class SharedDisk {
    @Id
    public String uuid;
    public String status;
    public long raw_capacity;//raw capacity in Byte
    public long object_size;
    @Column(name="coowner")
    public String coowner;
    @Transient
    public HashMap<String, String> devName = new HashMap<>(); //mapping from store ID to dev name on that store
}
