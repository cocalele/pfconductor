package com.netbric.s5.conductor.rpc;

import com.netbric.s5.conductor.RestfulReply;
import com.netbric.s5.orm.Tray;

import java.util.List;

public class ListDiskReply extends RestfulReply
{
    public ListDiskReply(String op, List<Tray> trays)
    {
        super(op);
        this.trays = trays;
    }
    public List<Tray> trays;
}
