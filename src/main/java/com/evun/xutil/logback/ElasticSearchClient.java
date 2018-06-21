package com.evun.xutil.logback;

import cn.evun.gap.common.utils.CollectionUtils;
import cn.evun.gap.core.exception.ServiceException;
import cn.evun.gap.eps.common.util.EpsJsonUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.net.URI;
import java.util.List;


/**
 * Created by Ni.MinJie on 2017/12/18.
 * ElasticSearch客户端
 */
public final class ElasticSearchClient {
    private final CloseableHttpClient httpClient;
    //例如: 12.43.34.34:9200
    private final HttpHost elasticsearchHost;

    public ElasticSearchClient(String hostname, Integer port) {
        this.elasticsearchHost = new HttpHost(hostname, port);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(5000)   //从连接池中获取连接的超时时间
                //与服务器连接超时时间：httpclient会创建一个异步线程用以创建socket连接，此处设置该socket的连接超时时间
                .setConnectTimeout(5000)
                .setSocketTimeout(15000)               //socket读数据超时时间：从服务器获取响应数据的超时时间
                .build();
        this.httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    /**
     * 批量插入es中
     *
     * @param list
     * @param <T>
     */
    public <T> void doBulkInsert(String index, String type, List<T> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        } else {
            CloseableHttpResponse response = null;
            try {
                HttpPost httpPost = new HttpPost();
                httpPost.setURI(new URI("_bulk"));
                httpPost.setHeader("Content-Type", "application/json;charset=UTF-8");
                StringBuilder entity = new StringBuilder();
                String prefix = String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}\n", index, type);
                //按照es的批量操作格式要求, 多个操作之间不需要分隔符号
                for (T elem : list) {
                    entity.append(prefix);
                    entity.append(EpsJsonUtils.toJSON(elem)).append("\n");
                }
                httpPost.setEntity(new StringEntity(entity.toString(), "UTF-8"));
                response = httpClient.execute(elasticsearchHost, httpPost);
            } catch (HttpHostConnectException hce) {
                return;
            } catch (Exception e) {
                throw new ServiceException(e);
            } finally {
                HttpClientUtils.closeQuietly(response);
            }
        }
    }

    /*public static void main(String[] args) {
        ElasticSearchClient searchBulk = new ElasticSearchClient("localhost", 9200);
        EpsSysLog sysLog = new EpsSysLog();
        //sysLog.set_index("yuncaijia-" + DateUtils.formatDate(new Date()));
        //sysLog.set_type("log");
        sysLog.setHostName("gl");
        sysLog.setLogLevel(10000);
        sysLog.setThreadName(Thread.currentThread().getName());
        //sysLog.setLogLevel(1000);
        sysLog.setLogTime(new Date());
        sysLog.setLogContent("这是我的第一条系统日志");
        List<EpsSysLog> list = new ArrayList<>();
        list.add(sysLog);
        searchBulk.doBulkInsert(list);
    }*/
}
