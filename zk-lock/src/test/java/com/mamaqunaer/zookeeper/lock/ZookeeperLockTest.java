package com.mamaqunaer.zookeeper.lock;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * ZookeeperLockTest.
 *
 * @author wangnan
 * @since 1.0.0, 2020/5/22
 **/
public class ZookeeperLockTest {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperLockTest.class);

    private ZookeeperLock zookeeperLock;

    @Before
    public void before() {
        this.zookeeperLock = new ZookeeperLock(new ZookeeperConnection("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"), "/curator/lock");
    }

    @Test
    public void testLockUtil() throws Exception {
        this.zookeeperLock.lock();
        try {
            // do my business
            logger.info("do my business..");
        } finally {
            zookeeperLock.unlock();
        }
    }

    @Test
    public void testLockTimeout() throws Exception {
        boolean locked = this.zookeeperLock.lock(Duration.ofMillis(10));
        if (!locked) {
            logger.warn("get lock failed.");
            return;
        }
        try {
            // do my business
            logger.info("do my business..");
        } finally {
            zookeeperLock.unlock();
        }
    }

    @Test
    @Ignore
    public void testParallel() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        for (int i = 0; i < 100000; i++) {
            executorService.execute(new Thread(() -> {
                try {
                    testLockTimeout();
//                    testLockUtil();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            try {
                TimeUnit.SECONDS.sleep(new Random().nextInt(2));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

}
