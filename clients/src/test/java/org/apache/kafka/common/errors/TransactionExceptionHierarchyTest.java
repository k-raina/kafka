/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.errors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionExceptionHierarchyTest {

    /**
     * Verifies that the given exception class extends `RetriableException`
     * and does **not** extend `RefreshRetriableException`.
     * Using `RefreshRetriableException` changes the exception handling behavior,
     * so only exceptions directly extending `RetriableException` are valid here.
     *
     * @param exceptionClass the exception class to check
     */
    private void assertRetriableExceptionInheritance(Class<?> exceptionClass) {
        assertTrue(RetriableException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should extend RetriableException");
        assertFalse(RefreshRetriableException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should NOT extend RefreshRetriableException");
    }

    @Test
    void testRetriableExceptionExceptionHierarchy() {
        assertRetriableExceptionInheritance(TimeoutException.class);
        assertRetriableExceptionInheritance(NotEnoughReplicasException.class);
        assertRetriableExceptionInheritance(CoordinatorLoadInProgressException.class);
        assertRetriableExceptionInheritance(CorruptRecordException.class);
        assertRetriableExceptionInheritance(NotEnoughReplicasAfterAppendException.class);
        assertRetriableExceptionInheritance(ConcurrentTransactionsException.class);
    }
}
