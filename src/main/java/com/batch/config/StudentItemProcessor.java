package com.batch.config;

import com.batch.entity.StudentEntity;
import org.springframework.batch.item.ItemProcessor;

public class StudentItemProcessor implements ItemProcessor<StudentEntity,StudentEntity> {
    @Override
    public StudentEntity process(StudentEntity student) {
        return student;
    }
}
