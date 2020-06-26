package com.netbric.s5.conductor.rpc;

import com.netbric.s5.conductor.RestfulReply;

public class ListStoreReply extends RestfulReply {
    public static class StoreInfo {
        public int id;
        public String status;
        public String mngt_ip;
    }
    public StoreInfo store_nodes[];

    public ListStoreReply(String op) {
        super(op);
    }

    public ListStoreReply(String op, int retCode, String reason) {
        super(op, retCode, reason);
    }

}
