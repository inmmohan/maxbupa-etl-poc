package com.springbatch.poc.springbatchpoc.configuration;


import com.springbatch.poc.springbatchpoc.PolicyItemProcessor;
import com.springbatch.poc.springbatchpoc.model.Policy;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
public class BatchConfigurationPolicy {

    @Autowired
    private MongoTemplate mongoTemplate;


    @Bean
    public DataSource dataSource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSourceBuilder.url("jdbc:sqlserver://10.210.16.46:1433;databaseName=maxbupa;integratedSecurity=false;");
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("citytech");
        return dataSourceBuilder.build();
    }

    @Bean(destroyMethod="")
    public JdbcCursorItemReader<Policy> policyReader(){
        JdbcCursorItemReader<Policy> reader = new JdbcCursorItemReader<Policy>();
        reader.setDataSource(dataSource());
        reader.setSql("SELECT policy_number, policy_type, timestamp FROM policy;");
        reader.setRowMapper(new BatchConfigurationPolicy.PolicyRowMapper());
        return reader;
    }

    public class PolicyRowMapper implements RowMapper<Policy> {

        @Override
        public Policy mapRow(ResultSet rs, int rowNum) throws SQLException {
            Policy policy = new Policy();
            policy.setId(rs.getString("policy_number"));
            policy.setPolicyNumber(rs.getString("policy_number"));
            policy.setPolicyType(rs.getString("policy_type"));
            policy.setTimestamp(rs.getString("timestamp"));
            return policy;
        }

    }

    @Bean
    public PolicyItemProcessor policyProcessor(){
        return new PolicyItemProcessor();
    }

    @Bean
    public MongoItemWriter<Policy> policyWriter() {
        MongoItemWriter<Policy> writer = new MongoItemWriter<Policy>();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("policy");
        return writer;
    }

    @Bean
    public Step policyStep1(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("step1").<Policy, Policy> chunk(10)
                .reader(policyReader())
                .processor(policyProcessor())
                .writer(policyWriter())
                .build();
    }

    @Bean
    public Job exportPolicyJob(JobBuilderFactory policyJob, Step policyStep1) {
        return policyJob.get("exportPolicyJob")
                .incrementer(new RunIdIncrementer())
                .flow(policyStep1)
                .end()
                .build();
    }
}
