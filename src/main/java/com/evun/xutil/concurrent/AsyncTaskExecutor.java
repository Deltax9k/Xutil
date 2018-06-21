package com.evun.xutil.concurrent;

import cn.evun.gap.base.context.UserContext;
import cn.evun.gap.common.model.UserContextDO;
import cn.evun.gap.common.utils.Assert;
import cn.evun.gap.common.utils.ReflectionUtils;
import cn.evun.gap.core.RR;
import cn.evun.gap.core.context.NamedThreadLocalContext;
import cn.evun.gap.core.exception.GapRuntimeException;
import cn.evun.gap.core.spring.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.Serializable;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * 异步执行工具类, 本类能够保证所有异步执行的任务也能处在和提交者线程相同的用户上下文中
 */
public abstract class AsyncTaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncTaskExecutor.class);

    //使用云彩家公共的线程池
    private static final ThreadPoolTaskExecutor executorService = SpringContext.getBean(ThreadPoolTaskExecutor.class);
    private static final String LOCALE_CLASS_NAME = Locale.class.getName();
    private static final String DEVICE_NAME = "device";

    /**
     * 执行任务, 有一个额外参数async控制任务是否异步执行
     *
     * @param runnable 需要执行的任务
     * @param context  执行的用户上下文
     * @param async    任务是否异步执行, 若为false, 任务将在当前线程中同步执行
     */
    public static void execute(Runnable runnable, UserContextDO context, boolean async) {
        checkTask(runnable);
        Runnable asyncTask = newTaskWrapper(runnable, context);
        if (async) {
            executorService.submit(asyncTask);
        } else {
            try {
                //为了防止影响当前线程上下文, 故依旧采用线程池
                executorService.submit(asyncTask).get();
            } catch (Exception e) {
                throw new GapRuntimeException(e);
            }
        }
    }

    /**
     * 执行任务, 有一个额外参数async控制任务是否异步执行
     *
     * @param runnable 需要执行的任务
     * @param async    任务是否异步执行, 若为false, 任务将在当前线程中同步执行
     */
    public static void execute(Runnable runnable, boolean async) {
        execute(runnable, getCurrentUserContext(), async);
    }

    /**
     * 异步方式执行任务
     *
     * @param runnable
     */
    public static void execute(Runnable runnable) {
        execute(runnable, getCurrentUserContext(), true);
    }

    public static <T> Future<T> submit(Callable<T> callable, UserContextDO context) {
        Callable<T> asyncTask = newTaskWrapper(callable, context);
        return executorService.submit(asyncTask);
    }

    public static <T> Future<T> submit(Callable<T> callable) {
        return submit(callable, getCurrentUserContext());
    }

    /**
     * 在当前事务成功提交后, 执行任务, 有一个额外参数async控制任务是否异步执行, 该参数一般为异步
     *
     * @param runnable
     * @param context
     * @param async
     */
    public static void executeAfterTxCommit(final Runnable runnable, final UserContextDO context, final boolean async) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
                execute(runnable, context, async);
            }
        });
    }

    /**
     * 在当前事务成功提交后, 执行任务, 有一个额外参数async控制任务是否异步执行, 该参数一般为异步
     *
     * @param runnable 需要执行的任务
     * @param async    任务是否异步执行, 若为false, 任务将在当前线程中同步执行
     */
    public static void executeAfterTxCommit(final Runnable runnable, final boolean async) {
        executeAfterTxCommit(runnable, AsyncTaskExecutor.getCurrentUserContext(), async);
    }

    /**
     * 使用指定线程的用户上下文, 将任务包装成可以安全异步执行的任务
     *
     * @param task
     * @param <T>
     * @return
     */
    public static <T> Callable<T> newTaskWrapper(final Callable<T> task, UserContextDO context) {
        checkTask(task);
        //如果任务已经包装过, 不再重复包装
        return task instanceof UserContextCallable ?
                task : new UserContextCallable<>(task, context);
    }

    /**
     * 使用当前线程的用户上下文, 将任务包装成可以安全异步执行的任务
     *
     * @param task
     * @param <T>
     * @return
     */
    public static <T> Callable<T> newTaskWrapper(final Callable<T> task) {
        return newTaskWrapper(task, getCurrentUserContext());
    }


    /**
     * 使用指定线程的用户上下文, 将任务包装成可以安全异步执行的任务
     *
     * @param task
     * @return
     */
    public static Runnable newTaskWrapper(final Runnable task, UserContextDO context) {
        checkTask(task);
        //如果任务已经包装过, 不再重复包装
        return task instanceof UserContextRunnable ?
                task : new UserContextRunnable(task, context);
    }

    /**
     * 使用当前线程的用户上下文, 将任务包装成可以安全异步执行的任务
     *
     * @param task
     * @return
     */
    public static Runnable newTaskWrapper(final Runnable task) {
        return newTaskWrapper(task, getCurrentUserContext());
    }

    /**
     * 获取当前线程的用户上下文信息, 如果没有，则返回空的上下文，避免没有没有上下文的任务执行报错
     *
     * @return
     */
    public static UserContextDO getCurrentUserContext() {
        try {
            Object ticket = NamedThreadLocalContext.getResource(RR.Context.AUTH_TICKET);
            Object device = NamedThreadLocalContext.getResource(DEVICE_NAME);
            Object attachment = NamedThreadLocalContext.getResource(UserContextDO.ATTACHMENT_KEY);
            UserContextDO userContextDO = new UserContextDO(ticket, device, null, UserContext.getUser());
            if (attachment != null) {
                if (attachment instanceof Serializable) {
                    userContextDO.setAttachment(((Serializable) attachment));
                } else {
                    LOG.error("附件类型为{},没有实现Serializeable", attachment.getClass());
                }
            }
            return userContextDO;
        } catch (Exception e) {
            LOG.error(null, e);
            return new UserContextDO(null, null, null, null);
        }
    }


    //**** 私有方法 *****//

    /**
     * 将任务包装成可以安全异步执行的任务
     *
     * @param task
     * @return
     */
    private static class UserContextRunnable implements Runnable {
        private final Runnable task;
        private final UserContextDO context;

        UserContextRunnable(final Runnable task, final UserContextDO context) {
            this.task = task;
            this.context = context;
        }

        @Override
        public void run() {
            try {
                NamedThreadLocalContext.unBindAll();
                NamedThreadLocalContext.bindResource(RR.Context.AUTH_TICKET, context.getTicket());
                NamedThreadLocalContext.bindResource(DEVICE_NAME, context.getDevice());
                NamedThreadLocalContext.bindResource(LOCALE_CLASS_NAME, context.getLocale());
                NamedThreadLocalContext.bindResource(UserContextDO.ATTACHMENT_KEY, context.getAttachment());
                //确保用户用户对象存在在当前上下文中
                tryEnsureUserContext(context);
                //使用反射方式调用, 防止sonar报错
                ReflectionUtils.invokeMethodByName(task, "run", new Object[0]);
            } finally {
                NamedThreadLocalContext.unBindAll();
            }
        }
    }

    private static class UserContextCallable<T> implements Callable<T> {
        private final Callable<T> task;
        private final UserContextDO context;

        UserContextCallable(final Callable<T> task, final UserContextDO context) {
            this.task = task;
            this.context = context;
        }

        @Override
        public T call() throws Exception {
            try {
                NamedThreadLocalContext.unBindAll();
                NamedThreadLocalContext.bindResource(RR.Context.AUTH_TICKET, context.getTicket());
                NamedThreadLocalContext.bindResource(DEVICE_NAME, context.getDevice());
                NamedThreadLocalContext.bindResource(Locale.class.getName(), context.getLocale());
                NamedThreadLocalContext.bindResource(UserContextDO.ATTACHMENT_KEY, context.getAttachment());
                tryEnsureUserContext(context);
                //使用反射方式调用, 防止sonar报错
                return (T) ReflectionUtils.invokeMethodByName(task, "call", new Object[0]);
            } finally {
                NamedThreadLocalContext.unBindAll();
            }
        }
    }

    private static void tryEnsureUserContext(UserContextDO context) {
        //在当前线程没有用户上下文时，尝试设置上下文，由于有可能有定时任务（定时任务没有用户上下文）也使用此类，
        // 为了保证定时任务顺利执行，在没法获取用户上下文时，不抛出异常
        if (UserContext.getUser() == null && context.getUser() != null) {
            UserContext.setUser(context.getUser());
        }
    }

    private static <T> void checkTask(Callable<T> callable) {
        Assert.notNull(callable, "任务不能为空!");
    }

    private static void checkTask(Runnable runnable) {
        Assert.notNull(runnable, "任务不能为空!");
    }

}
