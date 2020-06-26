package com.netbric.s5.conductor.rpc;

import com.netbric.s5.conductor.RestfulReply;

public class ListVolumeReply  extends RestfulReply {
    public static class VolumeInfo {
        public long id;
        public String name;
        public long size;
        public String status;
        public int rep_count;
    }

    public ListVolumeReply(String op) {
        super(op);
    }

    public ListVolumeReply(String op, int retCode, String reason) {
        super(op, retCode, reason);
    }

    public VolumeInfo[] volumes;
}
