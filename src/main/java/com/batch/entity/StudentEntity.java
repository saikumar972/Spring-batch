package com.batch.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
@AllArgsConstructor
@NoArgsConstructor
@Data
@Entity
public class StudentEntity {
    @Id
    private Long id;
    private String name;
    //@JsonFormat(shape = JsonFormat.Shape.STRING,pattern = "dd-MM-yyyy")
    private String joiningDate;
    private String course;
    private String subjects;
}
