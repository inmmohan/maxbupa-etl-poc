package com.springbatch.poc.springbatchpoc;


import com.springbatch.poc.springbatchpoc.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Autowired
    private MongoTemplate mongoTemplate;

    private JdbcTemplate jdbcTemplate;


    @Bean
    public DataSource dataSource() {
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSource.setUrl("jdbc:sqlserver://10.210.16.46:1433;databaseName=maxbupa;integratedSecurity=false;");
        dataSource.setUsername("sa");
        dataSource.setPassword("citytech");

        return dataSource;
    }

    @Bean
    public JdbcCursorItemReader<User> reader(){
        JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<User>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT id, name, city FROM person;");
        reader.setRowMapper(new UserRowMapper());

        return reader;
    }

    public class UserRowMapper implements RowMapper<User> {

        @Override
        public User mapRow(ResultSet rs, int rowNum) throws SQLException {
            User user = new User();
            user.setId(rs.getInt("id"));
            user.setName(rs.getString("name"));
            user.setCity(rs.getString("city"));
            return user;
        }

    }

    @Bean
    public UserItemProcessor processor(){
        return new UserItemProcessor();
    }

    @Bean
    public MongoItemWriter<User> writer() {
        MongoItemWriter<User> writer = new MongoItemWriter<User>();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("person");
        return writer;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1").<User, User> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job exportUserJob() {
        return jobBuilderFactory.get("exportUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }
}
