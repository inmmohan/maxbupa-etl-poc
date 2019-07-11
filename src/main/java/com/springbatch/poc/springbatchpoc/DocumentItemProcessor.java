package com.springbatch.poc.springbatchpoc;

import com.springbatch.poc.springbatchpoc.model.ScheduledJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;

public class DocumentItemProcessor implements Tasklet, StepExecutionListener {

    @Autowired
    private MongoTemplate mongoTemplate;

    private Logger logger = LoggerFactory.getLogger(
            DocumentItemProcessor.class);

    private ScheduledJob scheduledJob;



    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Processed Date :" + new Date());
        scheduledJob = new ScheduledJob();
        scheduledJob.setCreatedDate(new Date());
        scheduledJob.setId(1);
        scheduledJob.setName("ETLPROCESS");
        logger.debug("Lines Processor initialized.");
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution,
                                ChunkContext chunkContext) throws Exception {
        mongoTemplate.save(scheduledJob, "checkjobruntimestamp");
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.debug("Document Inserted in Mongo DB.");
        return ExitStatus.COMPLETED;
    }
}
