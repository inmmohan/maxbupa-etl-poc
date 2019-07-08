package com.springbatch.poc.springbatchpoc.configuration;


import com.springbatch.poc.springbatchpoc.PersonItemProcessor;
import com.springbatch.poc.springbatchpoc.model.Person;
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
public class BatchConfigurationPerson {

    @Autowired
    private MongoTemplate mongoTemplate;


    @Bean
    public DataSource dataSourcePerson() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSourceBuilder.url("jdbc:sqlserver://10.210.16.46:1433;databaseName=maxbupa;integratedSecurity=false;");
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("citytech");
        return dataSourceBuilder.build();
    }

    @Bean(destroyMethod="")
    public JdbcCursorItemReader<Person> reader(){
        JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<Person>();
        reader.setDataSource(dataSourcePerson());
        reader.setSql("SELECT id, name, city, timestamp FROM person;");
        reader.setRowMapper(new PersonRowMapper());
        return reader;
    }

    public class PersonRowMapper implements RowMapper<Person> {

        @Override
        public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
            Person person = new Person();
            person.setId(rs.getInt("id"));
            person.setName(rs.getString("name"));
            person.setCity(rs.getString("city"));
            person.setTimestamp(rs.getString("timestamp"));

            return person;
        }

    }

    @Bean
    public PersonItemProcessor processor(){
        return new PersonItemProcessor();
    }

    @Bean
    public MongoItemWriter<Person> writer() {
        MongoItemWriter<Person> writer = new MongoItemWriter<Person>();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("person");
        return writer;
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("step1").<Person, Person> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job exportPersonJob(JobBuilderFactory personJob, Step step1) {
        return personJob.get("exportPersonJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .end()
                .build();
    }
}
