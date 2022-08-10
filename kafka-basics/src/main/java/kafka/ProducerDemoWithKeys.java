package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());


    public static void main(String[] args) {

        log.info("Hi I am kafka producer1 ");


        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create producerRecord
        for (int i = 0; i <20 ; i++) {
              String topic = "topic_main";
              String key = "id_ " + i;
              String value = "message";

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key,value);

            //send the data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null ) {
                        log.info("Received new metadata  \n" +
                                "topic: " + metadata.topic() + "\n" +
                                "key_: " + producerRecord.key() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset : " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp()
                        );
                    }else {
                        log.error("error log" + exception);
                    }
                }
            });
        }
        //flush and close
        producer.flush();
        producer.close();
    }
}
