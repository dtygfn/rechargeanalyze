package kafkaproducer;

import conf.ConfigurationManager;
import constants.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

/**
 * 生产者，每10秒向kafka中发送一条数据
 */
public class CollectLog10 {
    public static void main(String[] args) {
        // 这个是用来配置kafka的参数
        Properties prop = new Properties();
        // 配置kafka中的broker列表
        prop.put(Constants.KAFKA_METADATA_BROKER_LIST, ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        //下面是分别配置 key和value的序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // 获取topic
        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);

        // 使用kafkaproducer 类来实例化来实现producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        try{
            // 读取hafs上的文件
            String cmccPath = ConfigurationManager.getProperty(Constants.CMCC_PATH);
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(cmccPath), configuration);
            FSDataInputStream fsDataInputStream = fs.open(new Path(cmccPath));
            BufferedReader bf = new BufferedReader(new InputStreamReader(fsDataInputStream,"utf-8"));
            String line = null;
            while((line=bf.readLine())!=null){
                // 设置每1秒读取一条数据
                Thread.sleep(1000);
                producer.send(new ProducerRecord<String, String>(topics, line));
            }
            bf.close();
            fsDataInputStream.close();
            fs.close();
            producer.close();
            System.out.println("已经发送完毕");

        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
