package com.jpmc.training.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.training.domain.Employee;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class EmployeeDeserializer  implements Deserializer<Employee> {
    private ObjectMapper mapper=new ObjectMapper();
    @Override
    public Employee deserialize(String topic, byte[] data) {
        // TODO Auto-generated method stub
        Employee employee=null;
        try {
            employee=mapper.readValue(data, Employee.class);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return employee;
    }

}