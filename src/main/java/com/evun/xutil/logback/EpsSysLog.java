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

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Date;

/**
 * 系统运行日志
 */
public final class EpsSysLog {
    /*//es索引
    private String _index;
    //文档类型
    private String _type;*/
    //日志id
    private Integer logId;
    //日志类
    private String logClass;
    //日志级别
    @JsonProperty("logLevel")
    private Integer logLevel;
    //日志内容
    private String logContent;
    //线程名称
    private String threadName;
    //日志时间
    private Date logTime;
    //主机名称
    private String hostName;

    /*public String get_index() {
        return _index;
    }

    public void set_index(String _index) {
        this._index = _index;
    }

    public String get_type() {
        return _type;
    }

    public void set_type(String _type) {
        this._type = _type;
    }*/

    public Integer getLogId() {
        return logId;
    }

    public void setLogId(Integer logId) {
        this.logId = logId;
    }

    public String getLogClass() {
        return logClass;
    }

    public void setLogClass(String logClass) {
        this.logClass = logClass;
    }

    public Integer getLogLevel() {
        return logLevel;
    }

    public String getLogLevelStr() {
        if (logLevel == null) {
            return "";
        }
        switch (logLevel) {
            case 5000:
                return "TRACE";
            case 10000:
                return "DEBUG";
            case 20000:
                return "INFO";
            case 30000:
                return "WARN";
            case 40000:
                return "ERROR";
            case 50000:
                return "FATAL";
            case 2147483647:
                return "OFF";
            default:
                return "";
        }
    }

    public void setLogLevel(Integer logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogContent() {
        return logContent;
    }

    public void setLogContent(String logContent) {
        this.logContent = logContent;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public Date getLogTime() {
        return this.logTime;
    }

    public void setLogTime(Date logTime) {
        this.logTime = logTime;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
}