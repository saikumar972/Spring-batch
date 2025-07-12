package com.batch.config;

import com.batch.entity.StudentEntity;
import com.batch.repo.StudentRepo;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CustomItemWriter<S> implements ItemWriter<StudentEntity> {
    @Autowired
    StudentRepo repo;
    @Override
    public void write(Chunk<? extends StudentEntity> chunk) throws Exception {
        System.out.println("Thread is "+Thread.currentThread().getName());
        repo.saveAll(chunk);
    }
}
