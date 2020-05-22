package com.mamaqunaer.zookeeper.lock;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


/**
 * Distributed lock based-on zookeeper.
 *
 * @author wangnan
 * @since 1.0.0, 2020/5/22
 **/
public class ZookeeperLock implements ZLock {

    private InterProcessMutex lock;

    public ZookeeperLock(ZookeeperConnection connection, String path) {
        this.lock = new InterProcessMutex(connection.getClient(), path);
    }

    @Override
    public void lock() throws Exception {
        this.lock.acquire();
    }

    @Override
    public boolean lock(Duration leaseTime) throws Exception {
        return this.lock.acquire(leaseTime.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void unlock() throws Exception {
        this.lock.release();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return this.lock.isOwnedByCurrentThread();
    }

}
