package io.upeksha.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * TODO: Class level comments please
 *
 * @author dimuthu
 * @since 1.0.0-SNAPSHOT
 */
public class SampleConsumer {
    public static void main(String args[]) {

        String topicName = "transaction-topic";
        int partitionId = 0;

        String groupName = "sample-group";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(200);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("Consuming message : " + record.value());
                        while (true) {
                            System.out.print("Do you want to consume this (y/n)?");
                            Scanner sc = new Scanner(System.in);
                            String input = sc.nextLine();

                            if ("y".equals(input)) {
                                break;
                            } else if ("n".equals(input)) {
                                System.out.println("Client failed before consume message : " + record.value());
                                throw new Exception("Manual interrupt by user");
                            }
                        }
                        long lastOffset = record.offset();
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                        System.out.println("Successfully consumed message : " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Interrupted");
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
