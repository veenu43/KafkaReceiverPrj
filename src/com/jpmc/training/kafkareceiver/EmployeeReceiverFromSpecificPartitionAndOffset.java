package com.jpmc.training.kafkareceiver;

import com.jpmc.training.deserializer.EmployeeDeserializer;
import com.jpmc.training.domain.Employee;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmployeeReceiverFromSpecificPartitionAndOffset {
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Properties props=new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EmployeeDeserializer.class.getName());


        KafkaConsumer<String, Employee> consumer=new KafkaConsumer<>(props);

        int partitionNo=Integer.parseInt(args[0]);
        int offset=Integer.parseInt(args[1]);
        List<TopicPartition> topicPartitions=new ArrayList();
        TopicPartition partition=new TopicPartition("emp-topic", partitionNo);
        topicPartitions.add(partition);

        consumer.assign(topicPartitions);
        consumer.seek(partition, offset);
        System.out.println("waiting for messages from partition "+partitionNo);
        while(true) {
            ConsumerRecords<String, Employee> records=consumer.poll(Duration.ofSeconds(20));
            records.forEach(record->{
                System.out.println("key: "+record.key()+" from partition: "+record.partition() +" at offset "+record.offset());
                System.out.println("Employee Details ");
                Employee employee=record.value();
                System.out.println("Id: "+employee.getId());
                System.out.println("Name: "+employee.getName());
                System.out.println("Designation: "+employee.getDesignation());
            });
        }

    }
}
