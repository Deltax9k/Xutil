package com.evun.xutil.springmvc;

import java.lang.annotation.*;

/**
 * Created by wq on 8/26/17.
 * 支持前台传来的两种格式的数据
 * {json: "...json字符串"} => 使用对象类型接收
 * {json: "[...多个json对象]"} => 使用 List<对象类型> 接收, 不支持其他集合类型, 包括数组[]
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface JsonString {
    //前台json字符串的key名, 不能为空/""
    String value();
}
