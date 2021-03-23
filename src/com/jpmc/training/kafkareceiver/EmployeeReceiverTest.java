package com.jpmc.training.kafkareceiver;

import com.jpmc.training.deserializer.EmployeeDeserializer;
import com.jpmc.training.domain.Employee;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmployeeReceiverTest 	{

        public static void main(String[] args) {
    // TODO Auto-generated method stub

    Properties props=new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EmployeeDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-1");

    KafkaConsumer<String, Employee> consumer=new KafkaConsumer<>(props);

    List<String> topics=new ArrayList();
    topics.add("emp-topic");

    consumer.subscribe(topics);
    System.out.println("waiting for messages");
    while(true) {
        ConsumerRecords<String, Employee> records=consumer.poll(Duration.ofSeconds(20));
        records.forEach(record->{
            System.out.println("key: "+record.key()+" from partition: "+record.partition());
            System.out.println("Employee Details ");
            Employee employee=record.value();
            System.out.println("Id: "+employee.getId());
            System.out.println("Name: "+employee.getName());
            System.out.println("Designation: "+employee.getDesignation());
        });
    }

}

    }