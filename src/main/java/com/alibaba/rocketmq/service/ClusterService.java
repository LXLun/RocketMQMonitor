package com.alibaba.rocketmq.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.UsualDate;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 集群服务类
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-8
 */
@Service
public class ClusterService extends AbstractService
{
    private Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
    private UsualDate ud = new UsualDate();
    private String endpoint = this.ud.getEndpoint();
    private String tags = "MQ监控";
    private String counterType = "GAUGE";
    private int step = 60;

    /**
     * 处理cluster数据
     * @return
     * @throws Throwable
     */
    public List<String> brokerDetail() throws Throwable
    {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = this.getDefaultMQAdminExt();
        try
        {
            //从运行中的MQ中得到数据
            defaultMQAdminExt.start();
            ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
            Set<Map.Entry<String, Set<String>>> clusterSet = clusterInfoSerializeWrapper.getClusterAddrTable().entrySet();
            Iterator<Map.Entry<String, Set<String>>> itCluster = clusterSet.iterator();
            while (itCluster.hasNext())
            {
                Map.Entry<String, Set<String>> next = itCluster.next();
                Set<String> brokerNameSet = new HashSet<String>();
                brokerNameSet.addAll(next.getValue());
                List<String> clusterList = new ArrayList<String>();
                //                Map<String, List<Map<String, String>>> brokerNameMap = new LinkedHashMap<String, List<Map<String, String>>>();
                for (String brokerName : brokerNameSet)
                {
                    String brokerBuild = this.buildMapToJson("brokerName", brokerName);
                    clusterList.add(brokerBuild);
                    //                    List<Map<String, String>> instanceList = new ArrayList<Map<String, String>>();
                    BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                    if (brokerData != null)
                    {
                        Set<Map.Entry<Long, String>> brokerAddrSet = brokerData.getBrokerAddrs().entrySet();
                        Iterator<Map.Entry<Long, String>> itAddr = brokerAddrSet.iterator();
                        while (itAddr.hasNext())
                        {
                            Map.Entry<Long, String> next1 = itAddr.next();
                            double in = 0;
                            double out = 0;
                            //                            String version = "";
                            long InTotalYest = 0;
                            long OutTotalYest = 0;
                            long InTotalToday = 0;
                            long OutTotalToday = 0;

                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            String putTps = kvTable.getTable().get("putTps");
                            String getTransferedTps = kvTable.getTable().get("getTransferedTps");
                            //                            version = kvTable.getTable().get("brokerVersionDesc");
                            {
                                String[] tpss = putTps.split(" ");
                                if (tpss != null && tpss.length > 0)
                                {
                                    in = Double.parseDouble(tpss[0]);
                                }
                            }

                            {
                                String[] tpss = getTransferedTps.split(" ");
                                if (tpss != null && tpss.length > 0)
                                {
                                    out = Double.parseDouble(tpss[0]);
                                }
                            }

                            String inTPSBuild = this.buildMapToJson("inTPS", in);
                            clusterList.add(inTPSBuild);
                            String outTPSBuild = this.buildMapToJson("outTPS", out);
                            clusterList.add(outTPSBuild);
                            //                            instanceMap.put("BID", str(next1.getKey().longValue()));
                            //                            instanceMap.put("Addr", next1.getValue());
                            //                            instanceMap.put("Version", version);
                            //                            instanceMap.put("inTPS", str(in));
                            //                            instanceMap.put("outTPS", str(out));
                            String msgPutTotalYesterdayMorning = kvTable.getTable().get("msgPutTotalYesterdayMorning");
                            String msgPutTotalTodayMorning = kvTable.getTable().get("msgPutTotalTodayMorning");
                            String msgPutTotalTodayNow = kvTable.getTable().get("msgPutTotalTodayNow");
                            String msgGetTotalYesterdayMorning = kvTable.getTable().get("msgGetTotalYesterdayMorning");
                            String msgGetTotalTodayMorning = kvTable.getTable().get("msgGetTotalTodayMorning");
                            String msgGetTotalTodayNow = kvTable.getTable().get("msgGetTotalTodayNow");

                            InTotalYest = Long.parseLong(msgPutTotalTodayMorning) - Long.parseLong(msgPutTotalYesterdayMorning);
                            OutTotalYest = Long.parseLong(msgGetTotalTodayMorning) - Long.parseLong(msgGetTotalYesterdayMorning);

                            InTotalToday = Long.parseLong(msgPutTotalTodayNow) - Long.parseLong(msgPutTotalTodayMorning);
                            OutTotalToday = Long.parseLong(msgGetTotalTodayNow) - Long.parseLong(msgGetTotalTodayMorning);

                            String inTotalYestBuild = this.buildMapToJson("InTotalYest", InTotalYest);
                            clusterList.add(inTotalYestBuild);
                            String outTotalYestBuild = this.buildMapToJson("OutTotalYest", OutTotalYest);
                            clusterList.add(outTotalYestBuild);

                            String inTotalTodayBuild = this.buildMapToJson("inTotalToday", InTotalToday);
                            clusterList.add(inTotalTodayBuild);
                            String outTotalTodayBuild = this.buildMapToJson("OutTotalYest", OutTotalToday);
                            clusterList.add(outTotalTodayBuild);

                            //                            instanceMap.put("InTotalYest", str(InTotalYest));
                            //                            instanceMap.put("OutTotalYest", str(OutTotalYest));
                            //                            instanceMap.put("InTotalToday", str(InTotalToday));
                            //                            instanceMap.put("OutTotalToday", str(OutTotalToday));
                            //                            instanceList.add(instanceMap);
                        }
                    }
                    //                    brokerNameMap.put(brokerName, instanceList);
                }
                return clusterList;
            }

        }
        catch (Throwable e)
        {
            t = e;
        }
        finally
        {
            this.shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }

    /**
     * 构建运维需要的每条数据的map，并且转化为Json返回
     * @param key
     * @param value
     * @return json
     * @throws UnknownHostException
     */
    private String buildMapToJson(String key, Object value)
    {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("endpoint", this.endpoint);
        map.put("tags", this.tags);
        map.put("timestamp", System.currentTimeMillis());
        map.put("metric", "MQ_" + key);
        map.put("value", value);
        map.put("counterType", this.counterType);
        map.put("step", this.step);
        String json = this.gson.toJson(map);

        return json;
    }

    /**
     * 向指定 URL 发送POST方法的请求
     * 
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     * @throws Throwable 
     */
    public String sendPost(String url, List<String> clusterList) throws Throwable
    {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try
        {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(clusterList);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null)
            {
                result += line;
            }
        }
        catch (Exception e)
        {
            System.out.println("发送 POST 请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输出流、输入流
        finally
        {
            try
            {
                if (out != null)
                {
                    out.close();
                }
                if (in != null)
                {
                    in.close();
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
        return result;
    }

}
