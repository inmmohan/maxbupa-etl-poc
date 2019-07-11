package com.springbatch.poc.springbatchpoc.configuration;


import com.springbatch.poc.springbatchpoc.BatchScheduler;
import com.springbatch.poc.springbatchpoc.DocumentItemProcessor;
import com.springbatch.poc.springbatchpoc.PersonItemProcessor;
import com.springbatch.poc.springbatchpoc.model.Person;
import com.springbatch.poc.springbatchpoc.model.ScheduledJob;
import org.bson.Document;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
@Import({BatchScheduler.class})
public class BatchConfigurationScheduledPerson {

    @Autowired
    private SimpleJobLauncher jobLauncher;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplateObject;

    @Value("${jobs.scheduled:true}")
    private boolean isEnabled;

    @Scheduled(cron = "*/10 * * * * *")
    public void perform() throws Exception {

        if (isEnabled) {
            System.out.println("Scheduled Job Started at :" + new Date());

            JobParameters param = new JobParametersBuilder().addString("JobID",
                    String.valueOf(System.currentTimeMillis())).toJobParameters();

            JobExecution execution = jobLauncher.run(exportScheduledPersonJob(), param);

            System.out.println("Job finished with status :" + execution.getStatus());
        }
    }


    @Bean
    public Job exportScheduledPersonJob() {
        return jobBuilderFactory.get("exportScheduledPersonJob")
                .start(scheduleStep2())
                .next(step3())
                .build();
    }

    @Bean
    public Step scheduleStep2() {
        return stepBuilderFactory.get("step1").<Person, Person> chunk(10)
                .reader(scheduledReader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Step step3() {
        System.out.println("Person export Job Started at :" + new Date());
        return stepBuilderFactory.get("step2")
                .tasklet(mongoTasklet())
                .build();
    }

    @Bean(destroyMethod="")
    @StepScope
    public JdbcCursorItemReader<Person> scheduledReader() {
        ScheduledJob scheduledJob = mongoTemplate.findOne(new Query(Criteria.where("id").is(1)), ScheduledJob.class, "checkjobruntimestamp");
        Date jobRunDate = scheduledJob.getCreatedDate();
        java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(jobRunDate.getTime());
        System.out.println("Timestamp::::::::"+sqlTimestamp);
        JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<Person>();
        reader.setSql("SELECT * FROM person WHERE timestamp >= DATEADD(minute, -5, ?);");
        reader.setDataSource(dataSource);
        reader.setPreparedStatementSetter(new ArgumentPreparedStatementSetter(new Timestamp[]{sqlTimestamp}));
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
    public MongoItemWriter<Person> writer() {
        MongoItemWriter<Person> writer = new MongoItemWriter<Person>();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("person");
        return writer;
    }

    @Bean
    public MongoItemWriter<Document> mongoItemwriter() {
        MongoItemWriter<Document> writer = new MongoItemWriter<Document>();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("person");
        return writer;
    }

    @Bean
    public PersonItemProcessor processor(){
        return new PersonItemProcessor();
    }

    @Bean
    public DocumentItemProcessor mongoTasklet(){
        return new DocumentItemProcessor();
    }

}
