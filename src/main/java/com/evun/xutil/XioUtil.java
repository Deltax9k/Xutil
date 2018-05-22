package com.evun.xutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by wq on 5/16/18.
 */
public abstract class XioUtil {
    private static final Logger log = LoggerFactory.getLogger(XioUtil.class);

    /**
     * 关闭所有流, 记录所有异常
     *
     * @param closeables
     */
    public static void closeQuietly(Closeable... closeables) {
        if (closeables != null && closeables.length > 0) {
            for (Closeable closeable : closeables) {
                if (closeable != null) {
                    if (closeable instanceof OutputStream) {
                        try {
                            ((OutputStream) closeable).flush();
                        } catch (Exception e) {
                            log.info("刷新流发生异常!", e);
                        }
                    }
                    try {
                        closeable.close();
                    } catch (Exception e) {
                        log.info("关闭流发生异常", e);
                    }
                }
            }
        }
    }

    /**
     * 删除文件或者文件夹，只有当所有的文件都删除成功，才返回true
     *
     * @param file
     * @return
     */
    public static boolean rm(File file) {
        if (file == null || !file.exists()) {
            return false;
        } else if (file.isFile()) {
            return file.delete();
        } else {
            File[] children = file.listFiles();
            if (children == null || children.length == 0) {
                return file.delete();
            } else {
                boolean success = true;
                for (File eachFile : children) {
                    if (success) {
                        success = rm(eachFile);
                    }
                }
                return file.delete() && success;
            }
        }
    }

    /**
     * 去除路径开头的/或者\, 同时去除../等有可能导致不安全的路径
     *
     * @param filePath
     * @return
     */
    public static String getSafePath(String filePath) {
        if (filePath != null) {
            filePath = filePath.replaceAll("[\\.]+[\\\\/]+", "");
            while (filePath.startsWith("\\") ||
                    filePath.startsWith("/")) {
                filePath = filePath.substring(1);
            }
        }
        return filePath;
    }

    /**
     * 获取正则化后的唯一确定 路径(去除../等)
     *
     * @param filePath
     * @return
     */
    public static String getCanonicalPath(String filePath) {
        if (filePath != null) {
            try {
                return new File(filePath).getCanonicalPath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }
}
