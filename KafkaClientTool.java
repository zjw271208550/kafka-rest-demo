package KafkaREST;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaClientTool<K,V> {
    private static Logger logger = LoggerFactory.getLogger(KafkaClientTool.class);

    private String HOST ;
    private int PORT ;
    private KafkaProducer<K,V> producer;
    private Callback callback;

    public KafkaClientTool(String HOST, int PORT) {
        this.HOST = HOST;
        this.PORT = PORT;
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST+":"+PORT);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,"10000");
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        prop.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"1048576");
        prop.put(ProducerConfig.LINGER_MS_CONFIG,"0");
        prop.put(ProducerConfig.ACKS_CONFIG,"1");
        //todo fasong pidaxiao peizhi
        //todo http pool
        prop.put(ProducerConfig.ACKS_CONFIG, "1");
        this.producer = new KafkaProducer<K, V>(prop);
        this.callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (null != exception) {
                    logger.error("发送消息失败",exception);
                }
            }
        };
    }

    public void sendMessage(String topic, int partition, K key,V val){
        Future rel = this.producer.send(
                new ProducerRecord<K, V>(topic,partition,key,val),
                this.callback
        );
        try {
            rel.get();
        }catch (Exception e){
            logger.error("发送失败",e);
        }
    }

}
