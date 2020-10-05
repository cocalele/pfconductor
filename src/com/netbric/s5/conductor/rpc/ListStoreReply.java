package com.netbric.s5.conductor.rpc;

import com.netbric.s5.orm.StoreNode;

import java.util.List;

public class ListStoreReply extends RestfulReply {

    public List<StoreNode> storeNodes;

    public ListStoreReply(String op) {
        super(op);
    }

    public ListStoreReply(String op, int retCode, String reason) {
        super(op, retCode, reason);
    }
    public ListStoreReply(String op, List<StoreNode> storeNodes) {
        super(op);
        this.storeNodes = storeNodes;
    }
}
