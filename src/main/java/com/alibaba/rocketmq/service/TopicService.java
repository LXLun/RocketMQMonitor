package com.alibaba.rocketmq.service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.admin.TopicOffset;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * topic信息类
 * @author luoxilun
 * @date 2016.11.22
 */
@Service
public class TopicService extends AbstractService
{
    /**
     * 过滤线上topicName
     * @return
     * @throws Throwable
     */
    public List<String> TopicListControl() throws Throwable
    {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = this.getDefaultMQAdminExt();
        try
        {
            //读取运行中的topicName
            defaultMQAdminExt.start();
            TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
            Set<String> topicset = topicList.getTopicList();
            List<String> setLists = new ArrayList<String>(topicset);
            //读取本地配置文件中的topicName
            Properties prop = new Properties();
            FileInputStream fis = new FileInputStream("topicNameList.properties");
            prop.load(fis);
            String topic = prop.getProperty("toplcNameList");
            //将数据数据根据","分隔，放入数组中
            String[] temp = topic.split(",");
            List<String> localLists = new ArrayList<String>();
            localLists.addAll(Arrays.asList(temp));
            //遍历setList集合
            List<String> filtList = new ArrayList<String>();
            for (int i = 0; i < setLists.size(); i++)
            {
                String setList = setLists.get(i);
                for (int j = 0; j < localLists.size(); j++)
                {
                    String localList = localLists.get(j);
                    if (setList.equals(localList))
                    {
                        filtList.add(setList);
                    }
                }
            }
            return filtList;
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

    public List<String> getTopicStats(String topicName) throws Throwable
    {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = this.getDefaultMQAdminExt();
        List<String> topicList = new ArrayList<String>();
        try
        {
            defaultMQAdminExt.start();
            TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topicName);
            List<MessageQueue> mqList = new LinkedList<MessageQueue>();
            mqList.addAll(topicStatsTable.getOffsetTable().keySet());
            Collections.sort(mqList);

            for (MessageQueue mq : mqList)
            {
                TopicOffset topicOffset = topicStatsTable.getOffsetTable().get(mq);
                topicList.add(this.buildMapToJson(topicName, "MinOffset", topicOffset.getMinOffset()));
                topicList.add(this.buildMapToJson(topicName, "MaxOffset", topicOffset.getMaxOffset()));
            }
            return topicList;
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
    private String buildMapToJson(String topicName, String key, Object value) throws UnknownHostException
    {
        InetAddress addr = InetAddress.getLocalHost();
        String endpoint = addr.getHostAddress();
        String tags = "MQ监控";
        String counterType = "GAUGE";
        int step = 60;

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("endpoint", endpoint);
        map.put("tags", tags);
        map.put("timestamp", System.currentTimeMillis());
        map.put("metric", "MQ_" + topicName + "_" + key);
        map.put("value", value);
        map.put("counterType", counterType);
        map.put("step", step);

        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        String json = gson.toJson(map);

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
    public String sendPost(String url, List<String> topicList) throws Throwable
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
            out.print(topicList);
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
