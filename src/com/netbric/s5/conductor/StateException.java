package com.netbric.s5.conductor;

public class StateException extends Exception{

    public int state=0;

    public StateException(String message)
    {
        super(message);
        // TODO Auto-generated constructor stub
    }
    public StateException(int errorState, String message)
    {
        super(message);
        state=errorState;
    }
}
