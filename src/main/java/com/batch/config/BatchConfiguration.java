package com.batch.config;

import com.batch.entity.StudentEntity;
import com.batch.service.ColumnRangePartitioner;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
@Log4j2
@AllArgsConstructor
public class BatchConfiguration {
    CustomItemWriter customItemWriter;
    JobRepository jobRepository;
    PlatformTransactionManager platformTransactionManager;
    @Bean
    public ItemReader<StudentEntity> itemReader(){
        log.info("BatchConfiguration : itemReader method start");
        FlatFileItemReader<StudentEntity> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("D:/Downloads/student_with_id.csv"));
        itemReader.setLinesToSkip(1);
        itemReader.setName("csv-reader");
        itemReader.setLineMapper(lineMapper());
        log.info("BatchConfiguration : itemReader method end");
        return itemReader;
    }

    private LineMapper<StudentEntity> lineMapper() {
        DefaultLineMapper<StudentEntity> lineMapper=new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","name","joiningDate","course","subjects");
        BeanWrapperFieldSetMapper<StudentEntity> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(StudentEntity.class);
      // fieldSetMapper.setConversionService(conversionService()); // Just this line added
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        log.info("BatchConfiguration : itemReader lineMapper method executed");
        return lineMapper;
    }
    @Bean
    public StudentItemProcessor itemProcessor(){
        log.info("BatchConfiguration : itemProcessor method");
        return new StudentItemProcessor();
    }

    @Bean
    public ItemWriter<StudentEntity> itemWriter(){
        log.info("BatchConfiguration : itemWriter method");
        return customItemWriter;
    }

    @Bean
    public Partitioner customPartitioner(){
        return new ColumnRangePartitioner();
    }

    @Bean
    public PartitionHandler customPartitionHandler(){
        TaskExecutorPartitionHandler partitionHandler=new TaskExecutorPartitionHandler();
        partitionHandler.setGridSize(4);
        partitionHandler.setStep(slaveStep());
        partitionHandler.setTaskExecutor(taskExecutor());
        return partitionHandler;
    }

    private Step masterStep() {
        return new StepBuilder("master-step",jobRepository)
                .partitioner(slaveStep().getName(),customPartitioner())
                .partitionHandler(customPartitionHandler())
                .build();
    }

    @Bean
    public Step slaveStep(){
        return new StepBuilder("slave-step",jobRepository).
                <StudentEntity,StudentEntity>chunk(250,platformTransactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job(){
        return new JobBuilder("job",jobRepository)
                .flow(masterStep())
                .end().build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor taskExecutor=new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setQueueCapacity(4);
        taskExecutor.setThreadNamePrefix("partition-thread-");
        taskExecutor.initialize();
        return taskExecutor;
//        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor=new SimpleAsyncTaskExecutor();
//        simpleAsyncTaskExecutor.setConcurrencyLimit(10);
//        return simpleAsyncTaskExecutor;
    }

/*    @Bean
    public ConversionService conversionService() {
        DefaultConversionService service = new DefaultConversionService();
        service.addConverter(String.class, LocalDate.class, source ->
                LocalDate.parse(source, DateTimeFormatter.ofPattern("dd-MM-yyyy")));
        return service;
    }*/

}
