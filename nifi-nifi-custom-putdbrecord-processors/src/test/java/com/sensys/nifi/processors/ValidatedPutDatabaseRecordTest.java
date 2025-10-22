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

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ValidatedPutDatabaseRecordTest {

    private TestRunner testRunner;
    
    @Mock
    private DBCPService mockEngineDbcp;
    
    @Mock
    private DBCPService mockSourceDbcp;
    
    @Mock
    private DBCPService mockTargetDbcp;

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);
        testRunner = TestRunners.newTestRunner(ValidatedPutDatabaseRecord.class);
    }

    @Test
    public void testValidationDisabled() {
        // Test that validation can be disabled
        testRunner.setProperty(ValidatedPutDatabaseRecord.ENABLE_VALIDATION, "false");
        testRunner.setProperty(ValidatedPutDatabaseRecord.DBCP_SERVICE, "mock-dbcp");
        testRunner.setProperty(ValidatedPutDatabaseRecord.TABLE_NAME, "test_table");
        
        // Should not require validation properties when disabled
        testRunner.assertValid();
    }

    @Test
    public void testValidationEnabledRequiresProperties() {
        // Test that validation properties are required when enabled
        testRunner.setProperty(ValidatedPutDatabaseRecord.ENABLE_VALIDATION, "true");
        testRunner.setProperty(ValidatedPutDatabaseRecord.DBCP_SERVICE, "mock-dbcp");
        testRunner.setProperty(ValidatedPutDatabaseRecord.TABLE_NAME, "test_table");
        
        // Should be invalid without validation properties
        testRunner.assertNotValid();
        
        // Add required validation properties
        testRunner.setProperty(ValidatedPutDatabaseRecord.OPERATION_ID, "${operation.id}");
        testRunner.setProperty(ValidatedPutDatabaseRecord.ENGINE_DBCP_SERVICE, "mock-engine-dbcp");
        testRunner.setProperty(ValidatedPutDatabaseRecord.SOURCE_DBCP_SERVICE, "mock-source-dbcp");
        testRunner.setProperty(ValidatedPutDatabaseRecord.TARGET_DBCP_SERVICE, "mock-target-dbcp");
        testRunner.setProperty(ValidatedPutDatabaseRecord.VALIDATION_MODE, "STRICT");
        
        testRunner.assertValid();
    }

    @Test
    public void testRelationships() {
        // Test that validation_failed relationship is included
        assertTrue(testRunner.getProcessor().getRelationships().contains(ValidatedPutDatabaseRecord.REL_VALIDATION_FAILED));
        assertTrue(testRunner.getProcessor().getRelationships().contains(ValidatedPutDatabaseRecord.REL_SUCCESS));
        assertTrue(testRunner.getProcessor().getRelationships().contains(ValidatedPutDatabaseRecord.REL_FAILURE));
        assertTrue(testRunner.getProcessor().getRelationships().contains(ValidatedPutDatabaseRecord.REL_RETRY));
    }

    @Test
    public void testPropertyDescriptors() {
        // Test that validation properties are included
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(ValidatedPutDatabaseRecord.ENABLE_VALIDATION));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(ValidatedPutDatabaseRecord.OPERATION_ID));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(ValidatedPutDatabaseRecord.ENGINE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(ValidatedPutDatabaseRecord.SOURCE_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(ValidatedPutDatabaseRecord.TARGET_DBCP_SERVICE));
        assertTrue(testRunner.getProcessor().getPropertyDescriptors().contains(ValidatedPutDatabaseRecord.VALIDATION_MODE));
    }

    @Test
    public void testValidationModeValues() {
        // Test validation mode allowable values
        testRunner.setProperty(ValidatedPutDatabaseRecord.VALIDATION_MODE, "STRICT");
        testRunner.assertValid();
        
        testRunner.setProperty(ValidatedPutDatabaseRecord.VALIDATION_MODE, "WARNING");
        testRunner.assertValid();
        
        testRunner.setProperty(ValidatedPutDatabaseRecord.VALIDATION_MODE, "INVALID");
        testRunner.assertNotValid();
    }

    @Test
    public void testOperationIdExpressionLanguage() {
        // Test that operation ID supports expression language
        testRunner.setProperty(ValidatedPutDatabaseRecord.OPERATION_ID, "${operation.id}");
        testRunner.assertValid();
        
        testRunner.setProperty(ValidatedPutDatabaseRecord.OPERATION_ID, "static-value");
        testRunner.assertValid();
    }
}
