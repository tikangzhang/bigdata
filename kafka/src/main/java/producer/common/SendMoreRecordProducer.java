package producer.common;

import java.text.SimpleDateFormat;
import java.util.Random;

public class SendMoreRecordProducer extends GenericProducer {
	private static final SimpleDateFormat STRING_TO_DATETIME= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String TOPIC = "mystate_raw";

    public SendMoreRecordProducer(String servers) {
        super(servers);
    }

    private void neverEnd(String[] ip,String[] id) throws InterruptedException {
        String  template = "{\"ip\":\"%s\",\"id\":\"%s\",\"topic\":\"state\", \"time\":\"%s\", \"aut\":\"MEMory\", \"run\":\"%s\", \"alarm\":\"***(Others)\", \"mp\":\"O6500\", \"sp\":\"O6620\", \"cnt\":%d, \"an\":[%s], \"am\":[], \"door\":%d, \"tml\":[2100,1575,2800,1100,1100,280,1500,500,2205,2000,500,1000,800,500,500,2000,2000,500,999,2200,0], \"tul\":[218,218,577,577,562,16,561,82,0,579,69,577,577,69,69,579,561,68,0,409,2100]}";
        int plusInterval = 5;
        int baseTime = 300;
        int index = 0;
        int cnt = 0,state = 0,alarmNo = 0,door = 0;
        String msg;
        long currentStamp;
        Random random = new Random();
        while(true){
            Thread.sleep(baseTime + random.nextInt(1000));
            currentStamp = System.currentTimeMillis();
            if(currentStamp % 600 == 0){
                cnt = -1;
            }

            if(currentStamp % 20 == 0){
                state = random.nextInt(4);
            }

            if (currentStamp % 10 == 0){
                door = 0;
            }else{
                door = 1;
            }

            if(currentStamp % 160 == 0){
                alarmNo = random.nextInt(10);
            }else if(currentStamp % 200 == 0){
                alarmNo = 0;
            }
            if(index < 0){
                cnt++;
                index = plusInterval;
            }

            for(int i = 0,len = ip.length;i < len; i++) {
                if(i > 2) {
                    msg = String.format(template, ip[i], id[i], STRING_TO_DATETIME.format(currentStamp), getState(state), cnt, getAlarm(alarmNo),door);
                }else{
                    msg = String.format(template, ip[i], id[i], STRING_TO_DATETIME.format(currentStamp - 100 * i), getState(state), cnt, getAlarm(alarmNo),door);
                }
                System.out.println(msg);
                send(TOPIC, msg);
            }
            index--;
        }
    }

    public static void main(String[] args) throws InterruptedException {

        SendMoreRecordProducer producer =  new SendMoreRecordProducer("192.168.2.213:9092");

        String[] ip = {"10.134.42.113","10.134.42.124","10.134.42.126","10.244.74.202","10.244.74.201","10.244.82.142","10.244.80.218","10.244.78.115","10.244.78.111","10.244.78.110"};
        String[] id = {"1-A8","1-A20","1-A22","3-F36","3-F37","3-G22","4-K45","1-G31","1-G35","1-G36"};

//        String[] ip = {"10.134.42.113","10.244.74.201","10.134.42.126"};
//        String[] id = {"1-A8","3-F37","1-A22"};
        producer.neverEnd(ip,id);

        producer.close();
    }

    public static String getState(int state){
        switch(state){
            case 0:
                return "STaRT";
            case 1:
                return "****(reset)";
            case 2:
                return "STOP";
            case 3:
                return "HOLD";
            case 4:
                return "MSTR(during retraction and re-positioning of tool retraction and recovery)";
            default:
                return "STaRT";
        }
    }

    public static String getAlarm(int order){
        if(order == 0){
            return "";
        }
        return "\"MC101" + order + "\"";
    }
}
