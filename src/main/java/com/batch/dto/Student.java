package com.batch.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.List;
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Student {
    private Long id;
    private String name;
    private LocalDate joiningDate;
    private String course;
    private List<String> subjects;
}
