package com.netbric.s5.orm;

import javax.persistence.Table;

/**
 * Created by liulele on 2019/11/19.
 */
@Table(name = "t_port")
public class Port {
    public static final int DATA = 0; //purpose normal access
    public static final int REPLICATING = 1;//purpose replicating access

    public String ip_addr;
    @javax.persistence.Transient
    public String name;
    public int purpose;
    public int store_id;
    public String status;

    public Port() {
    }
}
