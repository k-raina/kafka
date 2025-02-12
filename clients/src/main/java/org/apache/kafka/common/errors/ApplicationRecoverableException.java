package org.apache.kafka.common.errors;

/**
 * Indicates that the error is fatal to the producer, and the application
 * needs to restart the producer after handling the error. Depending on the application,
 * different recovery strategies (e.g., re-balancing task, restoring from checkpoints) may be employed.
 */
public abstract class ApplicationRecoverableException extends ApiException {
    public ApplicationRecoverableException(String message) {
        super(message);
    }
}
