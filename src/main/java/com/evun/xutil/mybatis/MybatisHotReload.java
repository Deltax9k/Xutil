package com.evun.xutil.mybatis;

import cn.evun.gap.common.utils.ReflectionUtils;
import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * Mybatis热更新支持
 *
 * @author ThinkGem 这个是原著的作者，我只是直接拿来改，原著莫怪
 * @version 2017-12-6
 */
public class MybatisHotReload implements InitializingBean, ApplicationContextAware {
    public static final Logger LOG = LoggerFactory.getLogger(MybatisHotReload.class);
    //需要修改的字段
    private static final String[] mapFieldNames = new String[]{
            "mappedStatements", "caches",
            "resultMaps", "parameterMaps",
            "keyGenerators", "sqlFragments"
    };
    //配置字段名称
    private static final String XML_SUFFIX = ".xml";

    private Configuration configuration;
    private String sqlSessionFactoryBeanName;
    //本地hostname与监视文件夹绝对路径对应关系
    private Map<String, String> hostNamelocalMonitorAbsolutePathMap;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SqlSessionFactory sessionFactory = (SqlSessionFactory) applicationContext.getBean(sqlSessionFactoryBeanName);
        this.configuration = sessionFactory.getConfiguration();
    }

    //监视某个文件(xml)并将发生变化的文件绝对路径发送出来
    private void doFileDetect(final Path root, final Debouncer<String> debouncer) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try (WatchService service = FileSystems.getDefault().newWatchService()) {
                    final Map<WatchKey, Path> directories = new HashMap<>();
                    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                            WatchKey watchKey = dir.register(
                                    service,
                                    new WatchEvent.Kind<?>[]{StandardWatchEventKinds.ENTRY_MODIFY},
                                    SensitivityWatchEventModifier.HIGH);
                            directories.put(watchKey, dir);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                    while (true) {
                        WatchKey watchKey = service.take();
                        List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                        for (WatchEvent<?> event : watchEvents) {
                            if (StandardWatchEventKinds.ENTRY_MODIFY.equals(event.kind())) {
                                Path dir = directories.get(watchKey);
                                if (dir == null) {
                                    continue;
                                }
                                Path fileName = dir.resolve((Path) event.context());
                                String absolutePath = fileName.toAbsolutePath().toString();
                                if (StringUtils.endsWithIgnoreCase(absolutePath, XML_SUFFIX)) {
                                    debouncer.put(absolutePath);
                                }
                            }
                        }
                        ErrorContext.instance().reset();
                        boolean reset = watchKey.reset();
                        if (!reset) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    LOG.error(null, e);
                }
            }
        }, "mybatishotreload-file-detector-thread").start();
    }

    //接收发生变化的xml文件, 尝试重新解析
    private void doMybatisRefresh(final Debouncer<String> debouncer) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    String absolutePath = debouncer.get();
                    String fileName = Paths.get(absolutePath).getFileName().toString();
                    try {
                        LOG.info("正在热更新文件: {} ({})", fileName, absolutePath);
                        //删除 resource, 让其重新加载
                        Set<String> loadedResources = (Set<String>)
                                ReflectionUtils.getFieldValue(configuration, "loadedResources");
                        loadedResources.remove(absolutePath);
                        //重新编译加载资源文件。
                        XMLMapperBuilder xmlMapperBuilder = new XMLMapperBuilder(
                                Files.newInputStream(Paths.get(absolutePath)),
                                configuration, absolutePath, configuration.getSqlFragments());
                        xmlMapperBuilder.parse();
                        LOG.info("[[成功]] 更新: {} ({})", fileName, absolutePath);
                    } catch (Exception e) {
                        LOG.error("[[错误]]更新: {} ({})", fileName, absolutePath, e);
                    }
                }
            }
        }, "mybatis-hotreload-refresh-thread").start();
    }

    public String getSqlSessionFactoryBeanName() {
        return sqlSessionFactoryBeanName;
    }

    public void setSqlSessionFactoryBeanName(String sqlSessionFactoryBeanName) {
        this.sqlSessionFactoryBeanName = sqlSessionFactoryBeanName;
    }

    public Map<String, String> getHostNamelocalMonitorAbsolutePathMap() {
        return hostNamelocalMonitorAbsolutePathMap;
    }

    public void setHostNamelocalMonitorAbsolutePathMap(Map<String, String> hostNamelocalMonitorAbsolutePathMap) {
        this.hostNamelocalMonitorAbsolutePathMap = hostNamelocalMonitorAbsolutePathMap;
    }

    private void initConfiguration(Configuration conf) {
        //替换原有ｍａｐ，　原因是原ｍａｐ不支持再次ｐｕｔ
        for (String fieldName : mapFieldNames) {
            Map old = (Map) ReflectionUtils.getFieldValue(conf, fieldName);
            Map<?, ?> newMap = new HashMap<>(old);
            ReflectionUtils.setFieldValue(conf, fieldName, newMap);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Path root;
        String hostName = (InetAddress.getLocalHost()).getHostName();
        if (hostNamelocalMonitorAbsolutePathMap != null &&
                hostNamelocalMonitorAbsolutePathMap.containsKey(hostName)) {
            root = Paths.get(hostNamelocalMonitorAbsolutePathMap.get(hostName));
        } else {
            //默认为当前类所在的文件夹
            root = Paths.get(getClass().getResource("/").toURI());
        }
        if (Files.exists(root)) {
            initConfiguration(configuration);
            Debouncer<String> debouncer = new Debouncer<>(1000);
            doFileDetect(root, debouncer);
            doMybatisRefresh(debouncer);
            LOG.info("Mybatis热部署启动成功! 正在监视文件夹: {}", root.toAbsolutePath().toString());
        } else {
            LOG.info("本地主机名称为: {}, 如需要支持Mybatis热部署, 请在eps-views.xml中配置相应的主机名称和监视文件夹绝对路径的对应关系!", hostName);
        }
    }

    /**
     * Created by wq on 12/6/17.
     * 线程安全的消除抖动集合
     * 所有放入的元素将在 refreshDelayMillis 后可以取出, 如果在 refreshDelayMillis 内有相同元素
     * 放入, 将重新计时, 不保证先放入的元素能够先取出.
     */
    private static class Debouncer<T> {
        private final long refreshDelayMillis;
        private final Map<T, Long> timeMap;

        public Debouncer(long refreshDelayMillis) {
            this.refreshDelayMillis = refreshDelayMillis;
            this.timeMap = new HashMap<>();
        }

        public synchronized void put(T elem) {
            Long old = timeMap.get(elem);
            long current = System.currentTimeMillis();
            if (old == null) {
                timeMap.put(elem, current);
            } else {
                if ((current - old) > refreshDelayMillis) {
                    //故意不更新, 让该值能够被读出
                } else {
                    //更新到当前时间戳
                    timeMap.put(elem, current);
                }
            }
            notify();
        }

        public synchronized T get() {
            while (true) {
                Set<Map.Entry<T, Long>> entries = timeMap.entrySet();
                if (entries.isEmpty()) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        continue;
                    }
                } else {
                    Iterator<Map.Entry<T, Long>> iterator = entries.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<T, Long> next = iterator.next();
                        T elem = next.getKey();
                        Long time = next.getValue();
                        if ((System.currentTimeMillis() - time) >= refreshDelayMillis) {
                            iterator.remove();
                            return elem;
                        } else {
                            continue;
                        }
                    }
                    try {
                        wait(refreshDelayMillis / 4);
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
            }
        }
    }
}