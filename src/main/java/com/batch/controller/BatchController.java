package com.batch.controller;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

@RestController
@RequestMapping("/batch")
@AllArgsConstructor
public class BatchController {
   JobLauncher jobLauncher;
   Job job;
    @SneakyThrows
    @PostMapping(path="/process")
    public void batchProcessing(@RequestParam("file")MultipartFile multipartFile){
        String fileName= multipartFile.getOriginalFilename();
        String TEMP_STORAGE = "D:\\Downloads\\temp";
        File fileToImport=new File(TEMP_STORAGE +fileName);
        multipartFile.transferTo(fileToImport);
        JobParameters jobParameters=new JobParametersBuilder()
                .addString("fullPathFileName", TEMP_STORAGE +fileName)
                .addLong("startAt",System.currentTimeMillis())
                .toJobParameters();
        JobExecution jobExecution=jobLauncher.run(job,jobParameters);
        if(jobExecution.getExitStatus().equals(ExitStatus.COMPLETED)){
            //delete the file in the local folder
            Files.deleteIfExists(Paths.get(TEMP_STORAGE+fileName));
        }
    }

}
