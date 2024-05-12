package com.netbric.s5.conductor;



public class PfcServer extends HTTPServer{

    public PfcServer(int port)
    {
        super(port);
        
    
    }
    public void addContext(String path, ContextHandler handler)
    {
        try {
            VirtualHost host = getVirtualHost(null); // default host
           
            host.addContext(path, handler);

        } catch (Exception e) {
            System.err.println("error: " + e);
        }
    }
}
