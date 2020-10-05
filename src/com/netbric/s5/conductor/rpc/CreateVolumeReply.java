package com.netbric.s5.conductor.rpc;

public class CreateVolumeReply extends RestfulReply {
    public long id;
    public String name;
    public long size;
    public String status;
    public int rep_count;

    public CreateVolumeReply(String op, com.netbric.s5.orm.Volume v) {
        super(op);
        id = v.id;
        name=v.name;
        size=v.size;
        status=v.status;
        rep_count = v.rep_count;
    }
}
