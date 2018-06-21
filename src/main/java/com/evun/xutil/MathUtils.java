package com.evun.xutil;

/**
 * Created by wq on 17-7-19.
 * 比较两个对象的数值是否相等（通过转化成字符串，故不管原始类型是否一致）
 * 比如：　１　＝＝　＂１＂　返回　ｔｒｕｅ
 * 　　　　１Ｌ　＝＝　new Integer(1) 返回　ｔｒｕｅ
 */
public final class MathUtils {
    /**
     * 值是否相等，忽略类型不一致，见类头部说明
     *
     * @param first
     * @param second
     * @return
     */
    public static boolean eq(Object first, Object second) {
        if (first == null && second == null) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }
        String value1 = first.toString();
        String value2 = second.toString();
        //对于字符串中含有．的情况，转化成Ｄｏｕｂｌｅ比较
        if (value1.contains(".") || value2.contains(".")) {
            return Double.valueOf(value1).equals(Double.valueOf(value2));
        }
        return value1.equalsIgnoreCase(value2);
    }

    /**
     * 值是否不相等
     *
     * @param first
     * @param second
     * @return
     */
    public static boolean nq(Object first, Object second) {
        return !eq(first, second);
    }

    private MathUtils() {
    }
}
