package com.mamaqunaer.zookeeper.lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

/**
 * Zookeeper connection.
 *
 * @author wangnan
 * @since 1.0.0, 2020/5/22
 **/
public class ZookeeperConnection {

    private static final int DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

    /**
     * list of servers to connect to
     */
    private String connectString = "127.0.0.1:2181";

    /**
     * retry policy (retry 2 times, interval 100 ms)
     */
    private final RetryPolicy retryPolicy = new RetryNTimes(2, 100);

    private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
    private int connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;

    private final CuratorFramework client;

    public ZookeeperConnection() {
        this.client = CuratorFrameworkFactory.builder().
                connectString(connectString).
                sessionTimeoutMs(sessionTimeoutMs).
                connectionTimeoutMs(connectionTimeoutMs).
                retryPolicy(retryPolicy).
                build();
        // the Curator Framework starts
        this.client.start();
    }

    public ZookeeperConnection(String connectString) {
        this.client = CuratorFrameworkFactory.builder().
                connectString(connectString).
                sessionTimeoutMs(sessionTimeoutMs).
                connectionTimeoutMs(connectionTimeoutMs).
                retryPolicy(retryPolicy).
                build();
        // the Curator Framework starts
        this.client.start();
    }

    public ZookeeperConnection(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy) {
        this.client = CuratorFrameworkFactory.builder().
                connectString(connectString).
                sessionTimeoutMs(sessionTimeoutMs).
                connectionTimeoutMs(connectionTimeoutMs).
                retryPolicy(retryPolicy).
                build();
        // the Curator Framework starts
        this.client.start();
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }
}
