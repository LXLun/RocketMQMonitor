package com.alibaba.rocketmq.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class UsualDate
{
    public static final String SEND_IP = "http://192.168.8.138:1988/v1/push";
    public String endpoint;

    public String getEndpoint()
    {
        return this.endpoint;
    }

    public void setEndpoint(String endpoint) throws UnknownHostException
    {
        InetAddress addr = InetAddress.getLocalHost();
        this.endpoint = addr.getHostAddress();
    }

}
