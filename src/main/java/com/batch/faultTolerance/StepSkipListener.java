package com.batch.faultTolerance;

import com.batch.entity.StudentEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.SkipListener;
@Log4j2
public class StepSkipListener implements SkipListener<StudentEntity,Number> {

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("Skipped during read due to: {}", t.getMessage(), t);
    }

    @Override
    public void onSkipInWrite(Number item, Throwable t) {
        log.warn("Skipped during write. Item: {} | Reason: {}", item, t.getMessage(), t);
    }

    @Override
    public void onSkipInProcess(StudentEntity item, Throwable t) {
        try {
            log.warn("Skipped during processing. StudentEntity: {} | Reason: {}", new ObjectMapper().writeValueAsString(item), t.getMessage(), t);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
