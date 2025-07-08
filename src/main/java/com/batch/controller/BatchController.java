package com.batch.controller;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
@AllArgsConstructor
public class BatchController {
   JobLauncher jobLauncher;
   Job job;
    @SneakyThrows
    @PostMapping("/process")
    public void batchProcessing(){
        JobParameters jobParameters=new JobParametersBuilder()
                .addLong("startAt",System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(job,jobParameters);
    }

}
