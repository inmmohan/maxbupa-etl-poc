package com.springbatch.poc.springbatchpoc;

import com.springbatch.poc.springbatchpoc.model.User;
import org.springframework.batch.item.ItemProcessor;

public class UserItemProcessor implements ItemProcessor<User, User> {

    @Override
    public User process(User user) throws Exception {
        return user;
    }
}
