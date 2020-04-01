package producer.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class SendOneRecordProducer extends GenericProducer {
	private static final SimpleDateFormat STRING_TO_DATETIME= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String TOPIC = "mystate_raw";

    public SendOneRecordProducer(String servers) {
        super(servers);
    }

    private void neverEnd(String[] ip,String[] id) throws InterruptedException {
        //String template = "{\"ip\":\"192.168.1.222\",\"cnt\":%d, \"logTime\":\"%s\"}";
        String  template = "{\"ip\":\"%s\",\"id\":\"%s\",\"topic\":\"state\", \"time\":\"%s\", \"aut\":\"MEMory\", \"run\":\"STaRT\", \"alarm\":\"***(Others)\", \"mp\":\"O6500\", \"sp\":\"O6620\", \"cnt\":%d, \"an\":[], \"am\":[], \"door\":1, \"tml\":[2100,1575,2800,1100,1100,280,1500,500,2205,2000,500,1000,800,500,500,2000,2000,500,999,2200,0], \"tul\":[218,218,577,577,562,16,561,82,0,579,69,577,577,69,69,579,561,68,0,409,2100]}";
        int plusInterval = 5;

        int index = 0;
        int cnt = 0;
        String msg;
        long currentStamp;
        while(true){
            Thread.sleep(3000);
            currentStamp = System.currentTimeMillis();
            if(currentStamp % 600 == 0){
                cnt = -1;
            }
            if(index < 0){
                cnt++;
                index = plusInterval;
            }

            for(int i = 0,len = ip.length;i < len; i++) {
                msg = String.format(template, ip[i], id[i], STRING_TO_DATETIME.format(currentStamp), cnt);
                System.out.println(msg);
                send(TOPIC, msg);
            }
            index--;
        }
    }
    private void doLoopFor(int loop) throws InterruptedException {
        String template1 = "{\"ip\":\"192.168.1.222\",\"cnt\":%d, \"logTime\":\"%s\"}";
        Random random = new Random();
        String msg;
        Date date = new Date();
        long now = System.currentTimeMillis();
        int cnt = 0;
        for (int i = 0; i < loop; i++) {
            now = now + random.nextInt(16000);
            date.setTime(now);
            if (i % 3 == 0) {
                cnt = 1;
            } else {
                cnt = 0;
            }
            msg = String.format(template1, cnt, STRING_TO_DATETIME.format(date));
            System.out.println(msg);
            send(TOPIC,msg);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        SendOneRecordProducer producer =  new SendOneRecordProducer("192.168.2.213:9092");

//        String ip = "10.134.42.113";
//        String id = "1-A8";
//        producer.neverEnd(ip,id);


        String[] ip = {"10.134.42.113","10.134.42.124","10.134.42.126","10.244.74.202","10.244.74.201"};
        String[] id = {"1-A8","1-A20","1-A22","3-F36","3-F37"};
        producer.neverEnd(ip,id);
        //producer.doLoopFor(3000);

        producer.close();
    }
}
