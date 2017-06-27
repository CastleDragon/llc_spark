import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2017/6/14.
 */
public class MakeData extends  Thread {
    private KafkaProducer ka;
    public Integer c_count;
    private final static Logger logger= LoggerFactory.getLogger(MakeData.class);

    public MakeData(){
        ka=new KafkaProducer();
        c_count=0;
    }

    public  void  run() {
        while (true) {
            c_count++;
            System.out.println(c_count);
            ka.produce(c_count.toString());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public void test(){
        System.out.println(ka.test());
    }


    public static void main(String[] args){
        MakeData makeData=new MakeData();
        makeData.start();
        logger.info("这里是一条info日志信息");
        logger.warn("这里是一条warn日志信息");

    }


}
