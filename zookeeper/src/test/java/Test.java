import com.laozhang.zookeeper.ZkClient;

public class Test {
    public static void main(String[] args) {
        try {
            ZkClient instance = ZkClient.getInstance();
            System.out.println(instance.getData());
            synchronized (instance) {
                while (!instance.dead()) {
                    instance.wait();
                }
            }
            System.out.println("Over!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
