package com.zhan.eshop.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author zhanzhan
 * @Date 2021/6/7 8:34
 */
public class MyArrayBlockingQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingQueue.class);


    /**
     * 阻塞队列
     *
     * 两个方法比较常使用，分别为put()和take():
     *
     * put()方法向队列中生产数据，当队列满时，线程阻塞
     *
     * take()方法从队列中消费数据，当队列为空是，线程阻塞
     */
    private static LinkedBlockingQueue frameTaskQueue = new LinkedBlockingQueue(500);
    private static ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    /**
     * <将消息放入队列中，若当前队列满，线程阻塞>
     *
     * @param msg msg
     * @throws
     */
    public static void produce(String msg) throws InterruptedException {
        System.out.println(msg);
        queue.put(msg);
    }

    /**
     * <消费队列中数据，若当前队列无数据，线程阻塞>
     *
     * @return 消息
     * @throws
     */
    public static String consume() throws InterruptedException {
        String msg = queue.take();
        System.out.println(msg);
        return msg;
    }


    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("====开始往queue里塞数====");
        for (int i = 0; i < 10; i++) {
            String msg = "hello world" + i;
            new Thread(() -> {
                try {
                    new MyArrayBlockingQueue().produce(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        TimeUnit.SECONDS.sleep(5L);
        LOGGER.info("====开始从queue里取数====");
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                try {
                    new MyArrayBlockingQueue().consume();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

}
