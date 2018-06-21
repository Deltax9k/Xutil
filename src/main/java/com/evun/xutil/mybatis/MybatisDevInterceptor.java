//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.evun.xutil.mybatis;

import cn.evun.gap.common.utils.DateUtils;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * mybatis插件, 用于打印完整sql和执行时间
 * 参考自: http://www.cnblogs.com/xrq730/p/6972268.html
 */
@Intercepts({
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
        @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class})
})
public class MybatisDevInterceptor implements Interceptor {
    private static final Logger LOG = LoggerFactory.getLogger(MybatisDevInterceptor.class);

    /**
     * 是否启用此拦截器
     */
    private boolean enabled = false;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement mappedStatement = (MappedStatement) invocation.getArgs()[0];
        Object parameter = null;
        if (invocation.getArgs().length > 1) {
            parameter = invocation.getArgs()[1];
        }
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Configuration configuration = mappedStatement.getConfiguration();

        long startTime = System.currentTimeMillis();
        Object result = null;
        try {
            result = invocation.proceed();
        } finally {
            try {
                long endTime = System.currentTimeMillis();
                long sqlCostTime = endTime - startTime;
                String sql = this.getSql(configuration, boundSql);
                this.formatSqlLog(mappedStatement.getSqlCommandType(), mappedStatement.getId(), sql, sqlCostTime, result);
            } catch (Exception e) {
                LOG.error("MybatisDevInterceptor注入参数异常!", e);
            }
        }

        return result;
    }

    @Override
    public Object plugin(Object target) {
        if (enabled && target instanceof Executor) {
            return Plugin.wrap(target, this);
        }
        return target;
    }

    @Override
    public void setProperties(Properties properties) {
    }

    /**
     * 获取完整的sql语句
     *
     * @param configuration
     * @param boundSql
     * @return
     */
    private String getSql(Configuration configuration, BoundSql boundSql) {
        // 输入sql字符串空判断
        String sql = boundSql.getSql();
        if (sql == null || sql.isEmpty()) {
            return "";
        }

        //美化sql
        sql = this.beautifySql(sql);

        //填充占位符, 目前基本不用mybatis存储过程调用,故此处不做考虑
        Object parameterObject = boundSql.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        if (!parameterMappings.isEmpty() && parameterObject != null) {
            TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
            if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                sql = this.replacePlaceholder(sql, parameterObject, "");
            } else {
                MetaObject metaObject = configuration.newMetaObject(parameterObject);
                for (ParameterMapping parameterMapping : parameterMappings) {
                    String propertyName = parameterMapping.getProperty();
                    if (metaObject.hasGetter(propertyName)) {
                        Object obj = metaObject.getValue(propertyName);
                        sql = this.replacePlaceholder(sql, obj, propertyName);
                    } else if (boundSql.hasAdditionalParameter(propertyName)) {
                        Object obj = boundSql.getAdditionalParameter(propertyName);
                        sql = this.replacePlaceholder(sql, obj, propertyName);
                    }
                }
            }
        }
        return sql;
    }

    /**
     * 美化Sql
     */
    private String beautifySql(String sql) {
        return sql.replaceAll("[\\s\n ]+", " ");
    }

    /**
     * 填充占位符?
     *
     * @param sql
     * @param parameterObject
     * @param propertyName
     * @return
     */
    private String replacePlaceholder(String sql, Object parameterObject, String propertyName) {
        String result;
        if (parameterObject instanceof String) {
            result = "'" + parameterObject.toString() + "'";
        } else if (parameterObject instanceof Date) {
            result = "'" + DateUtils.formatTime((Date) parameterObject) + "'";
        } else {
            result = parameterObject + "";
        }
        result = "/*" + propertyName + "*/" + result;

        //替换$符号时候出现的问题，先将$替换成字符串，后替换回来。
        result = result.replaceAll("\\$", "RDS_CHAR_DOLLAR");// encode replacement;
        String tempStr = sql.replaceFirst("\\?", result);
        tempStr = tempStr.replaceAll("RDS_CHAR_DOLLAR", "\\$");

        return tempStr;
    }

    /**
     * 格式化sql日志
     *
     * @param sqlCommandType
     * @param sqlId
     * @param sql
     * @param costTime
     * @return
     */
    private void formatSqlLog(SqlCommandType sqlCommandType, String sqlId, String sql, long costTime, Object obj) {
        if (sqlCommandType == SqlCommandType.UPDATE ||
                sqlCommandType == SqlCommandType.INSERT ||
                sqlCommandType == SqlCommandType.DELETE) {
            LOG.info("Spend Time:  {} ms, Affect Count:  {}, Mapper Method:  {}\n    {}\n", costTime, obj, sqlId, sql);
        } else {
            LOG.info("Spend Time:  {} ms, Mapper Method:  {}\n    {}\n", costTime, sqlId, sql);
        }
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}