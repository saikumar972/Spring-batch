package com.batch.config;

import com.batch.entity.StudentEntity;
import com.batch.repo.StudentRepo;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Configuration
//@EnableBatchProcessing
@AllArgsConstructor
public class BatchConfiguration {
    private StudentRepo studentRepo;
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
        RepositoryItemWriter<StudentEntity> itemWriter=new RepositoryItemWriter<>();
        itemWriter.setRepository(studentRepo);
        itemWriter.setMethodName("save");
        return itemWriter;
    }

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return new StepBuilder("step",jobRepository).
                <StudentEntity,StudentEntity>chunk(10,platformTransactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return new JobBuilder("job",jobRepository)
                .flow(step(jobRepository,platformTransactionManager))
                .end().build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor simpleasyncTaskExecutor=new SimpleAsyncTaskExecutor();
        simpleasyncTaskExecutor.setConcurrencyLimit(10);
        return simpleasyncTaskExecutor;
    }

//    @Bean
//    public ConversionService conversionService() {
//        DefaultConversionService service = new DefaultConversionService();
//        service.addConverter(String.class, LocalDate.class, source ->
//                LocalDate.parse(source, DateTimeFormatter.ofPattern("dd-MM-yyyy")));
//        return service;
//    }

}
