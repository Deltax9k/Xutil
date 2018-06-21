/*
 * Copyright 2009-2012 Evun Technology.
 *
 * This software is the confidential and proprietary information of
 * Evun Technology. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with evun.cn.
 */
package com.evun.xutil.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import cn.evun.gap.common.utils.CollectionUtils;
import cn.evun.gap.eps.util.ValidationUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.net.InetAddress.getLocalHost;

/**
 * 将日志异步批量记录到elasticsearch中的自定义logback appender
 * 特性:
 * 1. 异步, 批量
 * 2. 对于突发大量日志做了处理:
 * 一.超过80%队里容量, 抛弃新的低级别日志
 * 二.对于满容量队列, 批量抛弃一定量旧日志
 * 3. 日志发送延迟在1s左右(应当满足使用要求)
 * 4. 对于es宕机情况,在其恢复运行后, 能立即重连(其实是一直在尝试连接)
 * 5. 对于es配置错误情况, 不会影响业务系统正常运行
 */
@Component
public final class ElasticSearchAsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    //写入日志最大延迟(毫秒)
    private static final long MAX_REFRESH_MILLIS = 1000;
    //批量插入的最大条数
    private static final int MAX_BATCH_SIZE = 128;
    //logEvent队列容量
    private static final Integer MAX_QUEUE_SIZE = 256;
    //队列容量的80%, 当队列容量过大时,将抛弃info(含)级别以下的日志
    private static final Integer MAX_QUEUE_THREASHOLD = 256 / 10 * 8;
    //本地主机名称
    private static final String HOST_NAME;
    //每次丢弃的元素数量
    private static final int DTRAIN_ELEMENTS = 16;
    private static final int LOGGER_LEVEL_INFO = 20000;

    //从队列中取出消息的超时时间
    private static final int LOG_PULL_DELAY = 200;
    private static final String ROOT_LOGGER_NAME = "ROOT";
    private static final Logger ROOT_LOGGER = LoggerFactory.getLogger(ROOT_LOGGER_NAME);
    //logEvent缓冲队列
    private final ArrayBlockingQueue<ILoggingEvent> eventsQueue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    private int maxQueueSizeCount = 0;

    static {
        String tempHostName = "";
        try {
            tempHostName = getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            //忽略错误
        }
        HOST_NAME = tempHostName;
    }

    @Override
    public void start() {
        //只支持logback
        if (appenderEnabled &&
                StringUtils.isNotEmpty(elasticSearchIp) &&
                StringUtils.isNotEmpty(elasticSearchPort) &&
                ValidationUtils.isNumber(elasticSearchPort)) {
            //设置本appender为已经启动状态
            super.start();

            //启动消费者发送日志
            final ElasticSearchClient esClient = new ElasticSearchClient(
                    elasticSearchIp, Integer.parseInt(elasticSearchPort));
            //消费日志线程, 将日志批量写入到elasticsearch中
            Thread consumer = new Thread(new Runnable() {
                @Override
                public void run() {
                    doSendLogEvents(esClient);
                }
            });
            consumer.setDaemon(true);
            consumer.setName(this.getClass().toString());
            consumer.start();
            //将本appender加入到root logger中, 和配置文件中配置appender-ref效果类似
            //但这里为了与spring context对接, 只能选择在启动时通过反射动态加入到对象中
        }
    }

    /**
     * 由于某种原因, 队列可能会满, 为了阻塞业务线程, 采用丢弃旧日志的策略, 每次丢弃DTRAIN_ELEMENTS个
     *
     * @param eventObject
     */
    @Override
    protected void append(ILoggingEvent eventObject) {
        ArrayBlockingQueue<ILoggingEvent> evQueue = this.eventsQueue;
        if (evQueue.size() < MAX_QUEUE_THREASHOLD) {
            tryOfferOrDiscard(evQueue, eventObject);
        } else {
            if (eventObject.getLevel().toInt() <= LOGGER_LEVEL_INFO) {
                ROOT_LOGGER.info("由于队列太满丢失低级别日志!");
                //抛弃info(含)以下的记录
                return;
            } else {
                tryOfferOrDiscard(evQueue, eventObject);
            }
        }
    }

    /**
     * 尝试向队列放入日志, 或者抛弃旧的日志
     *
     * @param eventsQueue
     * @param eventObject
     */
    private void tryOfferOrDiscard(
            ArrayBlockingQueue<ILoggingEvent> eventsQueue,
            ILoggingEvent eventObject) {
        boolean success = false;
        try {
            success = eventsQueue.offer(eventObject, 200, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            ROOT_LOGGER.info("由于线程被打断而丢失数据!");
            //最后再尝试插入一次
            success = eventsQueue.offer(eventObject);
        }
        if (success) {
            int currentSize = eventsQueue.size();
            if (currentSize > maxQueueSizeCount) {
                maxQueueSizeCount = currentSize;
                ROOT_LOGGER.info("当前队列最大长度更新为: {}", maxQueueSizeCount);
            }
            return;
        } else {
            ROOT_LOGGER.info("由于超过200毫秒没有插入成功, 主动丢失旧日志!");
            //丢弃旧的日志, 每次丢弃16个
            List<ILoggingEvent> discarded = new ArrayList<>();
            eventsQueue.drainTo(discarded, DTRAIN_ELEMENTS);
            tryOfferOrDiscard(eventsQueue, eventObject);
        }
    }

    /**
     * 向es发送日志信息
     *
     * @param esClient
     */
    private void doSendLogEvents(ElasticSearchClient esClient) {
        List<EpsSysLog> logList = new ArrayList<>();
        long lastUpdate = System.currentTimeMillis();
        while (super.isStarted()) {
            try {
                //最可能的代码路径
                ILoggingEvent e = eventsQueue.poll();
                //如果失败, 则回退到定时的方式, 这样写是为了减少不必要的代码执行, 因为定时方式会有额外的计算
                if (e == null) {
                    e = eventsQueue.poll(LOG_PULL_DELAY, TimeUnit.MILLISECONDS);
                }
                if (e != null) {
                    EpsSysLog log = getEpsSysLog(e);
                    logList.add(log);
                }
                try {
                    //如果当前日志数量过大 或者 距离上一次写日志已经大于1s, 则尝试写日志
                    if (CollectionUtils.isNotEmpty(logList) &&
                            (logList.size() >= MAX_BATCH_SIZE
                                    || (System.currentTimeMillis() - lastUpdate) > MAX_REFRESH_MILLIS
                            )) {
                        lastUpdate = System.currentTimeMillis();
                        esClient.doBulkInsert(SysConfigCst.EPS_SYSLOG_ES_INDEX, SysConfigCst.EPS_SYSLOG_ES_TYPE, logList);
                        logList.clear();
                    } else {
                        continue;
                    }
                } catch (Exception ignored) {
                    //忽略错误, 不打印, 防止递归
                    //一旦异常, 应当是由于数量太多, 超过批量插入长度限制
                    logList.clear();
                    ROOT_LOGGER.error("由于异常丢失日志!");
                }
            } catch (InterruptedException ie) {
                break;
            }
        }
    }

    /**
     * 组装成 EpsSysLog 对象
     *
     * @param e
     * @return
     */
    private EpsSysLog getEpsSysLog(ILoggingEvent e) {
        EpsSysLog log = new EpsSysLog();
        log.setLogTime(new Date(e.getTimeStamp()));
        log.setLogClass(e.getLoggerName());
        log.setLogLevel(e.getLevel().toInteger());
        log.setThreadName(e.getThreadName());
        log.setHostName(HOST_NAME);

        //格式化异常栈信息
        if (e.getThrowableProxy() != null) {
            IThrowableProxy throwableProxy = e.getThrowableProxy();
            StringBuilder logContent = new StringBuilder();

            String formattedMessage = e.getFormattedMessage();
            if (StringUtils.isNotEmpty(formattedMessage)) {
                logContent.append(formattedMessage);
                if (!StringUtils.endsWith(formattedMessage, "\n")) {
                    logContent.append("\n");
                }
            }

            //加入错误信息
            String className = throwableProxy.getClassName();
            String message = throwableProxy.getMessage();
            if (StringUtils.isNotEmpty(className) && StringUtils.isNotEmpty(message)) {
                logContent.append(String.format("异常信息: %s: %s\n", className, message));
            }

            StackTraceElementProxy[] stackTraceElementProxyArray = throwableProxy.getStackTraceElementProxyArray();
            if (stackTraceElementProxyArray != null) {
                for (StackTraceElementProxy element : stackTraceElementProxyArray) {
                    String steAsString = element.getStackTraceElement().toString();
                    logContent.append(steAsString).append("\n");
                }
                log.setLogContent(logContent.toString());
                return log;
            } else {
                log.setLogContent(logContent.toString());
                return log;
            }
        } else {
            log.setLogContent(e.getFormattedMessage());
            return log;
        }
    }

    //是否启用本appender
    private boolean appenderEnabled;
    //es服务器ip地址
    private String elasticSearchIp;
    //es服务器端口号
    private String elasticSearchPort;

    public boolean isAppenderEnabled() {
        return appenderEnabled;
    }

    public void setAppenderEnabled(boolean appenderEnabled) {
        this.appenderEnabled = appenderEnabled;
    }

    public String getElasticSearchIp() {
        return elasticSearchIp;
    }

    public void setElasticSearchIp(String elasticSearchIp) {
        this.elasticSearchIp = elasticSearchIp;
    }

    public String getElasticSearchPort() {
        return elasticSearchPort;
    }

    public void setElasticSearchPort(String elasticSearchPort) {
        this.elasticSearchPort = elasticSearchPort;
    }
}
