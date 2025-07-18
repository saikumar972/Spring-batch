package com.batch.config;

import com.batch.entity.StudentEntity;
import com.batch.repo.StudentRepo;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class CustomItemWriter implements ItemWriter<StudentEntity>{
@Autowired
StudentRepo studentRepo;
    @Override
    public void write(Chunk<? extends StudentEntity>  students) {
        log.info("CustomItemWriter : Current Thread is {}", Thread.currentThread().getName());
        studentRepo.saveAll(students);
    }
}
