package com.alibaba.rocketmq.main;

import java.util.Scanner;

import com.alibaba.rocketmq.common.UsualDate;
import com.alibaba.rocketmq.config.ConfigureInitializer;
import com.alibaba.rocketmq.service.ClusterService;
import com.alibaba.rocketmq.service.TopicService;

public class MainRun
{

    /**
     * @param args
     * @throws Throwable 
     */
    public static void main(String[] args) throws Throwable
    {
        ClusterService cs = new ClusterService();
        TopicService ts = new TopicService();
        UsualDate ud = new UsualDate();
        ConfigureInitializer ci = new ConfigureInitializer();
        Scanner sc = new Scanner(System.in);
        String ip = sc.next();
        ci.setNamesrvAddr(ip + ":9876");
        ci.init();
        String sr = cs.sendPost(ud.SEND_IP, cs.brokerDetail());
        System.out.println(sr);
        for (String topicName : ts.TopicListControl())
        {
            String sr2 = ts.sendPost(ud.SEND_IP, ts.getTopicStats(topicName));
            System.out.println(sr2);
        }
    }
}
