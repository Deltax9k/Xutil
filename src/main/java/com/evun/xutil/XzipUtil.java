/*
 * Copyright 2009-2012 Evun Technology.
 *
 * This software is the confidential and proprietary information of
 * Evun Technology. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with evun.cn.
 */
package com.evun.xutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * XzipUtil
 *
 * @author ZENG.XIAO.YAN && ni.minjie
 * @version v1.0
 * @date 2018年5月19日 下午7:16:08
 */
public abstract class XzipUtil {
  private static final Logger log = LoggerFactory.getLogger(XzipUtil.class);

  private static final String FILE_SEPARATOR = "/";

  /**
   * 压缩成ZIP 方法1
   *
   * @param srcDirOrFile     压缩文件夹路径
   * @param destFile         目标压缩文件
   *                         false:所有文件跑到压缩包根目录下(注意：不保留目录结构可能会出现同名文件,会压缩失败)
   * @throws RuntimeException 压缩失败会抛出运行时异常
   */
  public static void zip(File srcDirOrFile, File destFile) {
    try {
      zip(srcDirOrFile, Files.newOutputStream(Paths.get(destFile.getCanonicalPath())));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void zip(File sourceFile, OutputStream out) {
    ZipOutputStream zos = null;
    try {
      long start = System.currentTimeMillis();
      zipInternal(sourceFile, zos = new ZipOutputStream(out), sourceFile.getName());
      log.debug("压缩完成，耗时：{} ms", (System.currentTimeMillis() - start));
    } catch (Exception e) {
      log.debug("压缩文件: {} 发生异常!", sourceFile.getAbsolutePath(), e);
    } finally {
      XioUtil.closeQuietly(zos);
    }
  }


  /**
   * 解压缩zip包
   *
   * @param zipFile  zip文件的全路径
   * @param unzipDir 解压后的文件保存的路径
   */
  @SuppressWarnings("unchecked")
  public static void unzip(File zipFile, File unzipDir) throws Exception {
    long start = System.currentTimeMillis();
    assertTrue(zipFile.isFile());
    assertTrue(unzipDir.isDirectory() || unzipDir.mkdirs());
    //开始解压
    ZipFile zip = new ZipFile(zipFile);
    Enumeration<ZipEntry> entries = (Enumeration<ZipEntry>) zip.entries();
    //循环对压缩包里的每一个文件进行解压
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      //构建压缩包中一个文件解压后保存的文件全路径
      File file = new File(unzipDir, entry.getName());
      file = new File(file.getCanonicalPath().replaceAll("\\\\", "/"));
      if (entry.isDirectory()) {
        assertTrue((file.isDirectory() || file.mkdirs()));
      } else {
        File parentFile = file.getParentFile();
        assertTrue((parentFile.isDirectory() || parentFile.mkdirs()));
        InputStream bis = null;
        try {
          bis = zip.getInputStream(entry);
          Files.copy(bis, Paths.get(file.getCanonicalPath()));
        } finally {
          XioUtil.closeQuietly(bis);
        }
      }
    }
    log.debug("解压缩完成，耗时：{} ms, 目录为：{}",
        System.currentTimeMillis() - start,
        unzipDir.getAbsolutePath());
  }

  /**
   * 递归压缩方法
   *
   * @param sourceFile       源文件
   * @param zos              zip输出流
   * @param name             压缩后的名称
   *                         false:所有文件跑到压缩包根目录下(注意：不保留目录结构可能会出现同名文件,会压缩失败)
   * @throws Exception
   */
  private static void zipInternal(File sourceFile, ZipOutputStream zos, String name) throws Exception {
    if (sourceFile.isFile()) {
      // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
      zos.putNextEntry(new ZipEntry(name));
      // copy文件到zip输出流中
      Files.copy(Paths.get(sourceFile.getCanonicalPath()), zos);
    } else {
      File[] listFiles = sourceFile.listFiles();
      if (listFiles == null || listFiles.length == 0) {
        // 需要保留原来的文件结构时,需要对空文件夹进行处理
        // 空文件夹的处理
        zos.putNextEntry(new ZipEntry(name + FILE_SEPARATOR));
        // 没有文件，不需要文件的copy
      } else {
        for (File file : listFiles) {
          // 判断是否需要保留原来的文件结构
          // 注意：file.getName()前面需要带上父文件夹的名字加一斜杠,
          // 不然最后压缩包中就不能保留原来的文件结构,即：所有文件都跑到压缩包根目录下了
          zipInternal(file, zos, name + FILE_SEPARATOR + file.getName());
        }
      }
    }
  }

  private static void assertTrue(boolean expression) {
    if (!expression) {
      throw new AssertionError();
    }
  }
}
