package com.springbatch.poc.springbatchpoc;

import com.springbatch.poc.springbatchpoc.model.Person;
import org.springframework.batch.item.ItemProcessor;

public class UserItemProcessor implements ItemProcessor<Person, Person> {

    @Override
    public Person process(Person user) throws Exception {
        return user;
    }
}
