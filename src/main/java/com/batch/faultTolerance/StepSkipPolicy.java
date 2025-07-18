package com.batch.faultTolerance;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

public class StepSkipPolicy implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        return t instanceof NumberFormatException && skipCount==5;
    }
}
