package com.batch.repo;

import com.batch.entity.StudentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StudentRepo extends JpaRepository<StudentEntity,Long> {
}
