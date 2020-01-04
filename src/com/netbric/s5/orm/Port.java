package com.netbric.s5.orm;

/**
 * Created by liulele on 2019/11/19.
 */
public class Port {
    public static final int NORMAL = 1; //purpose normal access
    public static final int REPLICATING = 2;//purpose replicating access

    public String ipv4;
    public String name;
    public int purpose;
    public int storeId;

    public Port(String ipv4, String name) {
        this.ipv4 = ipv4;
        this.name = name;
    }
}
