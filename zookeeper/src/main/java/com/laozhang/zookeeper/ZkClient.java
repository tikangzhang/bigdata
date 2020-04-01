package com.laozhang.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkClient implements Watcher {
    private String servers = "192.168.2.213:2181";
    private int timeOut = 5000;
    private String znode = "/sanguo";

    private static ZkClient instance;
    private static ZooKeeper zk;
    boolean dead = false;

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZkClient() throws Exception{
        zk = new ZooKeeper(servers, timeOut, this);
        waitingForConnected();
    }

    private void waitingForConnected()throws Exception{
        connectedSemaphore.await();
    }

    public List<String> getData() throws Exception{
        return zk.getChildren(this.znode, true);
    }
    public boolean dead(){
        return dead;
    }

    @Override
    public void process(WatchedEvent event){
        try {
            System.out.println("Call back: " + event.getState());
            Event.KeeperState state = event.getState();

            if(state == Event.KeeperState.AuthFailed || state == Event.KeeperState.Disconnected || state == Event.KeeperState.Expired ){
                dead = true;
                notifyAll();
            }
            if (state == Event.KeeperState.SyncConnected) {
                connectedSemaphore.countDown();
            }
            System.out.println(getData());
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public static ZkClient getInstance() throws Exception{
        if(instance == null){
            instance = new ZkClient();
        }
        return instance;
    }

}
