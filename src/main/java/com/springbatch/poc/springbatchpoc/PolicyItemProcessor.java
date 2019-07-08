package com.springbatch.poc.springbatchpoc;

import com.springbatch.poc.springbatchpoc.model.Policy;
import org.springframework.batch.item.ItemProcessor;

public class PolicyItemProcessor implements ItemProcessor<Policy, Policy> {

    @Override
    public Policy process(Policy policy) throws Exception {
        return policy;
    }
}
