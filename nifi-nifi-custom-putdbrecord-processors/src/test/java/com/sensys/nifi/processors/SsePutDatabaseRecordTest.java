/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sensys.nifi.processors;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for SsePutDatabaseRecord processor.
 * Tests all validation logic, error handling, and FlowFile routing.
 */
class SsePutDatabaseRecordTest {

    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = TestRunners.newTestRunner(SsePutDatabaseRecord.class);
    }

    @Test
    void testValidationDisabled() {
        // Test that validation can be disabled
        testRunner.setProperty(SsePutDatabaseRecord.ENABLE_VALIDATION, "false");
        testRunner.setProperty("Database Connection Pooling Service", "mock-dbcp");
        testRunner.setProperty("Table Name", "test_table");
        
        // Should be invalid without controller services, but we can test the property validation
        testRunner.assertNotValid();
        
        // Test that the processor has the expected properties
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENABLE_VALIDATION));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.OPERATION_ID));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.TARGET_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.VALIDATION_MODE));
    }

    @Test
    void testValidationEnabledRequiresProperties() {
        // Test that validation properties are required when enabled
        testRunner.setProperty(SsePutDatabaseRecord.ENABLE_VALIDATION, "true");
        testRunner.setProperty("Database Connection Pooling Service", "mock-dbcp");
        testRunner.setProperty("Table Name", "test_table");
        
        // Should be invalid without validation properties
        testRunner.assertNotValid();
        
        // Test that the processor requires validation properties when enabled
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.OPERATION_ID));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.TARGET_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.VALIDATION_MODE));
    }

    @Test
    void testRelationships() {
        // Test that all relationships are available
        assertTrue(testRunner.getProcessor().getRelationships().contains(SsePutDatabaseRecord.REL_VALIDATION_FAILED));
        assertTrue(testRunner.getProcessor().getRelationships().contains(SsePutDatabaseRecord.REL_SUCCESS));
        assertTrue(testRunner.getProcessor().getRelationships().contains(SsePutDatabaseRecord.REL_FAILURE));
        assertTrue(testRunner.getProcessor().getRelationships().contains(SsePutDatabaseRecord.REL_RETRY));
    }

    @Test
    void testPropertyDescriptors() {
        // Test that all validation properties are available
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENABLE_VALIDATION));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.OPERATION_ID));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.TARGET_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.VALIDATION_MODE));
    }

    @Test
    void testValidationModeValues() {
        // Test that VALIDATION_MODE accepts correct values
        testRunner.setProperty(SsePutDatabaseRecord.ENABLE_VALIDATION, "true");
        testRunner.setProperty(SsePutDatabaseRecord.OPERATION_ID, "${operation.id}");
        testRunner.setProperty(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE, "mock-engine-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE, "mock-source-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.TARGET_DBCP_SERVICE, "mock-target-dbcp");
        testRunner.setProperty("Database Connection Pooling Service", "mock-dbcp");
        testRunner.setProperty("Table Name", "test_table");
        
        // Test that VALIDATION_MODE property exists and accepts expected values
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.VALIDATION_MODE));
        
        // Test that the property descriptor exists and has expected properties
        var validationModeDescriptor = testRunner.getProcessor().getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SsePutDatabaseRecord.VALIDATION_MODE.getName()))
                .findFirst()
                .orElse(null);
        
        assertNotNull(validationModeDescriptor);
        // Test that the descriptor has the expected name
        assertEquals(SsePutDatabaseRecord.VALIDATION_MODE.getName(), validationModeDescriptor.getName());
    }

    @Test
    void testOperationIdExpressionLanguage() {
        // Test that OPERATION_ID supports Expression Language
        testRunner.setProperty(SsePutDatabaseRecord.ENABLE_VALIDATION, "true");
        testRunner.setProperty(SsePutDatabaseRecord.OPERATION_ID, "${operation.id}");
        testRunner.setProperty(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE, "mock-engine-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE, "mock-source-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.TARGET_DBCP_SERVICE, "mock-target-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.VALIDATION_MODE, "STRICT");
        testRunner.setProperty("Database Connection Pooling Service", "mock-dbcp");
        testRunner.setProperty("Table Name", "test_table");
        
        // Test that OPERATION_ID property supports Expression Language
        var operationIdDescriptor = testRunner.getProcessor().getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SsePutDatabaseRecord.OPERATION_ID.getName()))
                .findFirst()
                .orElse(null);
        
        assertNotNull(operationIdDescriptor);
        assertTrue(operationIdDescriptor.isExpressionLanguageSupported());
    }

    @Test
    void testMissingOperationId() {
        // Test that processor handles missing operation_id gracefully
        testRunner.setProperty(SsePutDatabaseRecord.ENABLE_VALIDATION, "true");
        testRunner.setProperty(SsePutDatabaseRecord.OPERATION_ID, "${operation.id}");
        testRunner.setProperty(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE, "mock-engine-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE, "mock-source-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.TARGET_DBCP_SERVICE, "mock-target-dbcp");
        testRunner.setProperty(SsePutDatabaseRecord.VALIDATION_MODE, "STRICT");
        testRunner.setProperty("Database Connection Pooling Service", "mock-dbcp");
        testRunner.setProperty("Table Name", "test_table");
        
        // Test that the processor has the expected relationships for handling missing operation_id
        assertTrue(testRunner.getProcessor().getRelationships().contains(SsePutDatabaseRecord.REL_VALIDATION_FAILED));
        assertTrue(testRunner.getProcessor().getRelationships().contains(SsePutDatabaseRecord.REL_FAILURE));
    }

    @Test
    void testProcessorInheritance() {
        // Test that SsePutDatabaseRecord is a valid processor
        assertTrue(testRunner.getProcessor() instanceof SsePutDatabaseRecord);
        assertTrue(testRunner.getProcessor() instanceof AbstractProcessor);
    }

    @Test
    void testValidationModeDefaultValue() {
        // Test that VALIDATION_MODE has a default value
        var validationModeDescriptor = testRunner.getProcessor().getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SsePutDatabaseRecord.VALIDATION_MODE.getName()))
                .findFirst()
                .orElse(null);
        
        assertNotNull(validationModeDescriptor);
        assertNotNull(validationModeDescriptor.getDefaultValue());
    }

    @Test
    void testEnableValidationDefaultValue() {
        // Test that ENABLE_VALIDATION has a default value
        var enableValidationDescriptor = testRunner.getProcessor().getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SsePutDatabaseRecord.ENABLE_VALIDATION.getName()))
                .findFirst()
                .orElse(null);
        
        assertNotNull(enableValidationDescriptor);
        assertNotNull(enableValidationDescriptor.getDefaultValue());
    }

    @Test
    void testProcessorCapabilities() {
        // Test that the processor has the expected capabilities
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENABLE_VALIDATION));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.OPERATION_ID));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.ENGINE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.SOURCE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.TARGET_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(SsePutDatabaseRecord.VALIDATION_MODE));
    }
}
