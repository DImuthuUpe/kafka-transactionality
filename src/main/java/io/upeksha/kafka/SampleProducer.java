package io.upeksha.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TODO: Class level comments please
 *
 * @author dimuthu
 * @since 1.0.0-SNAPSHOT
 */
public class SampleProducer {
    public static void main(String args[]) {

        String topicName = "transaction-topic";
        int partitionId = 0;
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(prop);

        int messageCounter = 0;
        String message;
        try {
            while (true) {
                message = "Message : " + messageCounter++;
                System.out.println("Producing message : " + message);
                producer.send(new ProducerRecord<String, String>(topicName, partitionId, "key", message));
                Thread.sleep(3000);
            }

        } catch (Exception ex) {
            System.out.println("Interrupted");
            ex.printStackTrace();
        } finally {
            System.out.println("Closing producer");
            producer.close();
        }
    }
}
