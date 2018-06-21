package com.evun.xutil.redis;

import cn.evun.gap.common.utils.Assert;
import cn.evun.gap.common.utils.ReflectionUtils;
import cn.evun.gap.common.utils.StringUtils;
import cn.evun.gap.core.cache.support.redis.jedis.JedisClient;
import cn.evun.gap.core.spring.SpringContext;
import cn.evun.gap.eps.ba.controller.MaterialController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

/**
 * Ｒｅｄｉｓ工具类，用于保证多实例(多个jvm实例)情况下的并发操作
 * 使用示例 (1):
 * <p>
 * boolean success = RedisUtils.tryLockAndRun("mylock", DEFAULT_MIN_EXPIRE_SECS, new Runnable() {
 *
 * @Override public void run() {
 * //业务逻辑代码, 必须是同步的
 * }
 * }
 * <p>
 * if (success) {
 * //执行成功的操作
 * } else {
 * //执行失败的操作
 * }
 * <p>
 * <p>
 * 使用示例 (2):
 * <p>
 * RedisUtils.Result<String> result = RedisUtils.tryLockAndRun("mylock", DEFAULT_MIN_EXPIRE_SECS, new Callable<String>() {
 * @Override public String call() {
 * //业务逻辑代码, 必须是同步的
 * return "success";
 * }
 * }
 * <p>
 * if (result.isDone()) {
 * //务必先调用result的isDone方法判断是否成功执行
 * String result = result.getResult();
 * //对结果做处理
 * } else {
 * //执行失败的操作
 * }
 */
public abstract class RedisUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MaterialController.class);

    private static final int DEFAULT_EXPIRE_SECS = 3600 * 12;//默认过期时间(12小时)
    private static final int DEFAULT_MIN_EXPIRE_SECS = 60;
    private static final int DEFAULT_MIN_RUN_SECS = 0;
    private static final String REDIS_LOCK_PREFIX = RedisUtils.class.getName() + ".lock.";//锁名称的前缀, 所有锁都会加上此前缀
    private static final JedisClient jedisClient = SpringContext.getBean(JedisClient.class);

    /****** 以下为Runnable参数的多个重载方法 *****/

    /**
     * 使用指定过期时间尝试获取锁并执行任务, 如果获取锁失败, 则任务将不被执行, 并返回false
     *
     * @param lockName   锁名称
     * @param expireSecs 设置锁的过期时间(超过该时间,锁会自动释放)
     * @param minRunSecs 最小运行时间(如果获取锁成功, 不论任务是否发生异常, 本方法总会在该最小运行时间后返回;
     *                   如果获取锁失败, 任务会立即返回, 最小运行时间被忽略)
     * @param task       　需要执行的任务
     * @return 获取锁成功且任务执行成功(无异常), 返回(最小运行时间之后)true；　否则返回false
     */
    public static boolean tryLockAndRun(String lockName, int expireSecs, int minRunSecs, final Runnable task) {
        Result<Void> result = tryLockAndRun(lockName, expireSecs, minRunSecs, new CallableAdapter(task));
        return result.isDone();
    }

    /**
     * 使用固定的过期时间(DEFAULT_MIN_EXPIRE_SECSs)尝试获取锁并执行任务, 如果获取锁失败, 则任务将不被执行, 并返回false
     * 此方法和tryLockAndRun(String lockName, final Callable<T> task) 方法类似
     * 一般用于控制执行时间较短(几秒)的任务的跨(jvm)实例并发执行,
     *
     * @param lockName 锁名称
     * @param task     　需要执行的任务
     * @return 获取锁成功且任务执行成功(无异常), 立即返回(忽略最小运行时间)true；
     * 获取锁成功但任务执行异常, 抛出异常
     * 获取锁失败,返回false;
     */
    public static boolean tryLockAndRun(String lockName, final Runnable task) {
        Result<Void> result = tryLockAndRun(lockName, DEFAULT_MIN_EXPIRE_SECS, DEFAULT_MIN_RUN_SECS, new CallableAdapter(task));
        return result.isDone();
    }

    /**
     * 使用默认过期时间(12小时)尝试获取锁并执行任务, 如果获取锁失败, 则任务将不被执行, 并返回false
     *
     * @param lockName   锁名称
     * @param minRunSecs 最小运行时间(如果获取锁成功, 不论任务是否发生异常, 本方法总会在该最小运行时间后返回;
     *                   如果获取锁失败, 任务会立即返回, 最小运行时间被忽略)
     * @param task       　需要执行的任务
     * @return 获取锁成功且任务执行成功(无异常), 返回(最小运行时间之后)true； 获取锁失败,返回false; 任务执行异常, 抛出异常
     */
    public static boolean tryLockAndRun(String lockName, int minRunSecs, final Runnable task) {
        Result<Void> result = tryLockAndRun(lockName, DEFAULT_EXPIRE_SECS, minRunSecs, new CallableAdapter(task));
        return result.isDone();
    }

    /****** 以下为Callable参数的多个重载方法 *****/

    /**
     * 使用指定过期时间尝试获取锁并执行任务, 如果获取锁失败, 任务将不被执行, 并返回false
     *
     * @param lockName   　需要获取的锁名称
     * @param expireSecs 　过期时间(单位:秒), 超过这个时间,锁自动被释放
     * @param minRunSecs 　任务最小运行时间, 这个值用来保证该方法的最小运行时间(即使任务运行时发生异常, 该方法也会在等待到最小运行时间才返回)
     * @param task       　需要执行的任务
     * @return 获取锁成功，　isDone返回true, getResult返回执行结果；　获取锁失败, isDone返回false, getReuslt方法抛出异常
     */
    public static <T> Result<T> tryLockAndRun(String lockName, int expireSecs, int minRunSecs, final Callable<T> task) {
        Assert.isTrue(StringUtils.isNotEmpty(lockName), "无效的空锁名！");
        Assert.isTrue(expireSecs > 0, "过期时间必须大于0秒!");
        Assert.isTrue(minRunSecs >= 0, "最小运行时间必须大于0秒!");
        Assert.isTrue(expireSecs >= minRunSecs, "锁过期时间必须大于最短运行时间!");
        Assert.isTrue(task != null, "任务不能为空!");

        final String fullLockName = getfullLockName(lockName);
        final long deadline = System.currentTimeMillis() + minRunSecs * 1000;
        boolean locked = lockAndSetExpireInternal(fullLockName, expireSecs);
        if (locked) {
            Future<T> future = AsyncTaskExecutor.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    try {
                        return (T) ReflectionUtils.invokeMethodByName(task, "call", new Object[0]);
                    } finally {
                        long timeToWait = deadline - System.currentTimeMillis();
                        try {
                            //先休眠, 再释放资源, 如果执行有异常, 也会保证最小运行时间
                            if (timeToWait > 0) {
                                Thread.sleep(timeToWait);
                            }
                        } catch (InterruptedException e) {
                            //在finally块中, 不应该抛出异常
                            Thread.currentThread().interrupt();
                        } finally {
                            //释放锁和释放redis连接分别用try-catch包起来, 防止前者发生异常导致后者不执行, 导致连接泄露
                            Jedis resource = null;
                            try {
                                resource = jedisClient.getResource();
                                resource.del(fullLockName);
                            } catch (Exception e) {
                                LOG.error("释放锁： " + fullLockName + "　发生异常！", e);
                            } finally {
                                if (resource != null) {
                                    jedisClient.returnResource(resource);
                                }
                            }
                        }
                    }
                }
            });
            return new Result<>(future, true);
        }
        return new CancelledResult<>();
    }

    /**
     * 使用固定的过期时间(DEFAULT_MIN_EXPIRE_SECSs)尝试获取锁并执行任务, 如果获取锁失败, 则任务将不被执行
     * 此方法和tryLockAndRun(String lockName, final Runnable task) 方法类似
     * 一般用于控制执行时间较短(几秒)的任务的跨(jvm)实例并发执行
     *
     * @param lockName 锁名称
     * @param task     　需要执行的任务
     * @return 获取锁成功且任务执行成功(无异常)，　isDone返回true, getResult返回执行结果；
     * 　      获取锁成功但任务执行异常, 直接抛出异常;
     * 获取锁失败, isDone返回false, getReuslt方法抛出异常
     */
    public static <T> Result<T> tryLockAndRun(String lockName, Callable<T> task) {
        return tryLockAndRun(lockName, DEFAULT_MIN_EXPIRE_SECS, DEFAULT_MIN_RUN_SECS, task);
    }

    /**
     * 使用默认过期时间(12小时)尝试获取锁并执行任务, 如果获取锁失败, 则任务将不被执行, 并返回false
     *
     * @param lockName   锁名称
     * @param minRunSecs 最小运行时间(如果获取锁成功, 不论任务是否发生异常, 本方法总会在该最小运行时间后返回;
     *                   如果获取锁失败, 任务会立即返回, 最小运行时间被忽略)
     * @param task       　需要执行的任务
     * @return 获取锁成功，　isDone返回true, getResult返回执行结果；　获取锁失败, isDone返回false, getReuslt方法抛出异常
     */
    public static <T> Result<T> tryLockAndRun(String lockName, int minRunSecs, Callable<T> task) {
        return tryLockAndRun(lockName, DEFAULT_EXPIRE_SECS, minRunSecs, task);
    }

    /**
     * 尝试获取锁, 如果成功则同时设置过期时间, 最后返回是否获取成功的结果
     *
     * @param fullLockName
     * @param resource
     * @param expireSecs
     * @return
     */
    private static boolean lockAndSetExpireInternal(String fullLockName, int expireSecs) {
        Jedis resource = null;
        try {
            resource = jedisClient.getResource();
            //使用redis的setnx原子操作, 设置某个值, 如果操作成功, 将返回1, 则认为获取锁成功
            boolean success = MathUtils.eq(1, resource.setnx(fullLockName, "1"));
            if (success) {
                //如果没有过期时间, 可能发生意外锁没有被释放, 从而永远无法再次获取的情况
                resource.expire(fullLockName, expireSecs);
            }
            return success;
        } finally {
            if (resource != null) {
                jedisClient.returnResource(resource);
            }
        }
    }

    //获取完整锁名
    private static String getfullLockName(String lockName) {
        return REDIS_LOCK_PREFIX + lockName;
    }

    /**
     * 将Runnable对象包装成Callable对象
     *
     * @param task 非空对象
     * @return
     */
    private static class CallableAdapter implements Callable<Void> {
        private final Runnable task;

        public CallableAdapter(Runnable task) {
            Assert.notNull(task, "任务不能为空!");
            this.task = task;
        }

        @Override
        public Void call() throws Exception {
            //使用反射调用, 防止sonar提示不能调用run方法的错误
            ReflectionUtils.invokeMethodByName(task, "run", new Object[0]);
            return null;
        }
    }

    public static class Result<T> {
        private final Future<T> result;
        private final boolean isDone;

        public Result(Future<T> result, boolean isDone) {
            this.result = result;
            this.isDone = isDone;
        }

        public Future<T> getResult() {
            return result;
        }

        public boolean isDone() {
            return isDone;
        }
    }

    private static class CancelledResult<T> extends Result<T> {

        public CancelledResult() {
            super(null, false);
        }

        @Override
        public Future<T> getResult() {
            throw new CancellationException("由于获取锁失败, 任务已经被取消!");
        }
    }
}
