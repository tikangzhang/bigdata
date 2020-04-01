package producer.file;

import producer.common.GenericProducer;
import java.io.*;
import java.text.SimpleDateFormat;

public class SendRecordFromFileProducer extends GenericProducer {
	private static final SimpleDateFormat STRING_TO_DATETIME= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String TEMPLATE = "{\"ip\":\"%s\",\"id\":\"%s\",%s}";
    private static final String TOPIC = "mystate_raw";

    public SendRecordFromFileProducer(String servers) {
        super(servers);
    }

    public void sendFromFile(String patchName,String ip,String id){
        BufferedReader br = null;
        try{
            String data;
            InputStreamReader reader = new InputStreamReader(new FileInputStream(new File(patchName)));
            br = new BufferedReader(reader);
            String line = br.readLine();
            while (line != null) {
                line = String.format(TEMPLATE,ip,id,line.substring(line.indexOf("\"topic\"")));
                send(TOPIC,line);
                System.out.println(line);
                line = br.readLine(); // 一次读入一行数据
            }
            close();
        }catch(Exception ex){
            ex.printStackTrace();
        }finally {
            if(null != br){
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws InterruptedException {
        SendRecordFromFileProducer producer =  new SendRecordFromFileProducer("192.168.2.213:9092");

        //String fileName = "C:\\Users\\admin\\Desktop\\now\\Data\\10.134.88.116\\test.txt";
        String fileName = "C:\\Users\\admin\\Desktop\\now\\凯胜数据\\Data\\10.134.88.116\\StateInfo_0.txt";
        String ip = "10.134.42.118";
        String id = "1-A13";

        producer.sendFromFile(fileName,ip,id);
        producer.close();
    }
}
