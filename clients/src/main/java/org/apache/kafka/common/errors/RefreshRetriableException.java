package org.apache.kafka.common.errors;

/**
 * Indicates that an operation failed due to outdated or invalid metadata,
 * requiring a refresh (e.g., refreshing producer metadata) before retrying the request.
 * The request can be modified or updated with fresh metadata before being retried.
 */
public abstract class RefreshRetriableException extends RetriableException {
    public RefreshRetriableException(String message) {
        super(message);
    }
}
