package com.batch.config;

import com.batch.entity.StudentEntity;
import com.batch.repo.StudentRepo;
import com.batch.service.ColumnRangePartitioner;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
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
//@EnableBatchProcessing
@AllArgsConstructor
public class BatchConfiguration {
    private CustomItemWriter itemWriter;
    @Bean
    public ItemReader<StudentEntity> itemReader(){
        FlatFileItemReader<StudentEntity> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/student_with_id.csv"));
        itemReader.setLinesToSkip(1);
        itemReader.setName("csv-reader");
        itemReader.setLineMapper(lineMapper());
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
        return lineMapper;
    }
    @Bean
    public StudentItemProcessor itemProcessor(){
        return new StudentItemProcessor();
    }

    @Bean
    public ItemWriter<StudentEntity> itemWriter(){
        return new CustomItemWriter<StudentEntity>();
    }

    @Bean
    public Partitioner customPartitioner(){
        return new ColumnRangePartitioner();
    }

    @Bean
    public PartitionHandler customPartitionHandler(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        TaskExecutorPartitionHandler partitionHandler=new TaskExecutorPartitionHandler();
        partitionHandler.setGridSize(4);
        partitionHandler.setStep(slaveStep(jobRepository,platformTransactionManager));
        partitionHandler.setTaskExecutor(taskExecutor());
        return partitionHandler;
    }

    private Step masterStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("master-step",jobRepository)
                .partitioner(slaveStep(jobRepository,platformTransactionManager).getName(),customPartitioner())
                .partitionHandler(customPartitionHandler(jobRepository,platformTransactionManager))
                .build();
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return new StepBuilder("slave-step",jobRepository).
                <StudentEntity,StudentEntity>chunk(250,platformTransactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return new JobBuilder("job",jobRepository)
                .flow(masterStep(jobRepository,platformTransactionManager))
                .end().build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
//        SimpleAsyncTaskExecutor simpleasyncTaskExecutor=new SimpleAsyncTaskExecutor();
//        simpleasyncTaskExecutor.setConcurrencyLimit(10);
//        return simpleasyncTaskExecutor;
        ThreadPoolTaskExecutor taskExecutor=new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setQueueCapacity(4);
        taskExecutor.setThreadNamePrefix("partition-thread-");
        return taskExecutor;
    }

//    @Bean
//    public ConversionService conversionService() {
//        DefaultConversionService service = new DefaultConversionService();
//        service.addConverter(String.class, LocalDate.class, source ->
//                LocalDate.parse(source, DateTimeFormatter.ofPattern("dd-MM-yyyy")));
//        return service;
//    }

}
