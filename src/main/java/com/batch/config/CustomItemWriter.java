package com.batch.config;

import com.batch.entity.StudentEntity;
import com.batch.repo.StudentRepo;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CustomItemWriter implements ItemWriter<StudentEntity>{
@Autowired
StudentRepo studentRepo;
    @Override
    public void write(Chunk<? extends StudentEntity> students) throws Exception {
        System.out.println("Current Thread is "+Thread.currentThread().getName());
        studentRepo.saveAll(students);
    }
}
