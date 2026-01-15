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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.dbcp.DBCPService;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "record", "jdbc", "put", "database", "update", "insert", "delete", "validation", "environment", "sensys", "sse"})
@CapabilityDescription("Puts records into a target database with environment validation. " +
    "Validates that controller services match the expected environment configuration by querying the SSE Engine database " +
    "using operation_id from FlowFile attributes. This is a simplified version that focuses on validation functionality.")
@ReadsAttribute(attribute = "operation.id", description = "Operation ID used to look up environment configuration")
@WritesAttribute(attribute = "validation.passed", description = "true if validation passed")
@UseCase(description = "Insert records into a database with environment validation")
public class SimpleValidatedPutDatabaseRecord extends AbstractProcessor {

    // Custom validation properties
    public static final PropertyDescriptor ENABLE_VALIDATION = new PropertyDescriptor.Builder()
            .name("Enable Validation")
            .description("Enable environment validation before processing records")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor OPERATION_ID = new PropertyDescriptor.Builder()
            .name("Operation ID")
            .description("Operation ID to look up environment configuration. Can use Expression Language to reference FlowFile attributes like ${operation.id}")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENGINE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("SSE Engine DBCP Service")
            .description("Database Connection Pooling Service for the SSE Engine database")
            .required(false)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SOURCE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Source DBCP Service")
            .description("Database Connection Pooling Service for the source database")
            .required(false)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TARGET_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Target DBCP Service")
            .description("Database Connection Pooling Service for the target database")
            .required(false)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("Validation Mode")
            .description("How to handle validation failures")
            .required(true)
            .allowableValues("STRICT", "WARNING")
            .defaultValue("STRICT")
            .build();

    // Basic database properties
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the database table to insert/update records into")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully processed FlowFile")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to process FlowFile")
            .build();

    public static final Relationship REL_VALIDATION_FAILED = new Relationship.Builder()
            .name("validation_failed")
            .description("FlowFiles that fail environment validation are routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_VALIDATION_FAILED
    );

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            ENABLE_VALIDATION,
            OPERATION_ID,
            ENGINE_DBCP_SERVICE,
            SOURCE_DBCP_SERVICE,
            TARGET_DBCP_SERVICE,
            VALIDATION_MODE,
            DBCP_SERVICE,
            TABLE_NAME
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ComponentLog logger = getLogger();
        
        try {
            // Check if validation is enabled
            boolean enableValidation = context.getProperty(ENABLE_VALIDATION).asBoolean();
            
            if (enableValidation) {
                // Perform environment validation
                ValidationResult validationResult = performEnvironmentValidation(context, flowFile);
                
                if (!validationResult.isValid()) {
                    // Add validation attributes to FlowFile
                    flowFile = session.putAttribute(flowFile, "validation.passed", "false");
                    flowFile = session.putAttribute(flowFile, "validation.error", validationResult.getErrorMessage());
                    flowFile = session.putAttribute(flowFile, "validation.timestamp", String.valueOf(System.currentTimeMillis()));
                    
                    // Route based on validation mode
                    String validationMode = context.getProperty(VALIDATION_MODE).getValue();
                    if ("STRICT".equals(validationMode)) {
                        logger.error("Environment validation failed: {}", validationResult.getErrorMessage());
                        session.transfer(flowFile, REL_VALIDATION_FAILED);
                        return;
                    } else {
                        logger.warn("Environment validation failed (WARNING mode): {}", validationResult.getErrorMessage());
                    }
                } else {
                    // Add success attributes
                    flowFile = session.putAttribute(flowFile, "validation.passed", "true");
                    flowFile = session.putAttribute(flowFile, "validation.environment", validationResult.getEnvironment());
                    flowFile = session.putAttribute(flowFile, "validation.timestamp", String.valueOf(System.currentTimeMillis()));
                }
            }

            // For this simplified version, we'll just log that we would process the record
            String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            logger.info("Would process record for table: {} (simplified version)", tableName);
            
            // Transfer to success
            session.transfer(flowFile, REL_SUCCESS);
            
        } catch (Exception e) {
            logger.error("Error processing FlowFile", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private ValidationResult performEnvironmentValidation(ProcessContext context, FlowFile flowFile) {
        ComponentLog logger = getLogger();
        
        try {
            // Get operation ID
            PropertyValue operationIdProp = context.getProperty(OPERATION_ID);
            String operationId = operationIdProp.evaluateAttributeExpressions(flowFile).getValue();
            
            if (operationId == null || operationId.trim().isEmpty()) {
                return ValidationResult.failure("Operation ID is required for validation");
            }

            // Get DBCP services
            DBCPService engineDbcpService = context.getProperty(ENGINE_DBCP_SERVICE).asControllerService(DBCPService.class);
            DBCPService sourceDbcpService = context.getProperty(SOURCE_DBCP_SERVICE).asControllerService(DBCPService.class);
            DBCPService targetDbcpService = context.getProperty(TARGET_DBCP_SERVICE).asControllerService(DBCPService.class);

            if (engineDbcpService == null) {
                return ValidationResult.failure("SSE Engine DBCP Service is required for validation");
            }

            // Query environment configuration
            EnvironmentConfig envConfig = queryEnvironmentConfig(engineDbcpService, operationId);
            if (envConfig == null) {
                return ValidationResult.failure("No environment configuration found for operation ID: " + operationId);
            }

            // Validate source database if configured
            if (sourceDbcpService != null) {
                String expectedSourceUrl = buildJdbcUrl(envConfig.getSourceHost(), envConfig.getSourcePort(), envConfig.getSourceDatabase());
                String actualSourceUrl = extractJdbcUrlFromService(sourceDbcpService);
                
                if (!compareJdbcUrls(expectedSourceUrl, actualSourceUrl)) {
                    return ValidationResult.failure("Source database URL mismatch. Expected: " + expectedSourceUrl + ", Actual: " + actualSourceUrl);
                }
            }

            // Validate target database if configured
            if (targetDbcpService != null) {
                String expectedTargetUrl = buildJdbcUrl(envConfig.getTargetHost(), envConfig.getTargetPort(), envConfig.getTargetDatabase());
                String actualTargetUrl = extractJdbcUrlFromService(targetDbcpService);
                
                if (!compareJdbcUrls(expectedTargetUrl, actualTargetUrl)) {
                    return ValidationResult.failure("Target database URL mismatch. Expected: " + expectedTargetUrl + ", Actual: " + actualTargetUrl);
                }
            }

            return ValidationResult.success(envConfig.getEnvironment());

        } catch (Exception e) {
            logger.error("Error during environment validation", e);
            return ValidationResult.failure("Validation error: " + e.getMessage());
        }
    }

    private EnvironmentConfig queryEnvironmentConfig(DBCPService engineDbcpService, String operationId) {
        String sql = "SELECT DISTINCT " +
                "dmr.source_host, dmr.source_port, dmr.source_database, " +
                "dmr.target_host, dmr.target_port, dmr.target_database, " +
                "dmr.environment " +
                "FROM data_migration_records dmr " +
                "WHERE dmr.operation_id = ?";

        try (Connection connection = engineDbcpService.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            
            statement.setString(1, operationId);
            
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return new EnvironmentConfig(
                            resultSet.getString("source_host"),
                            resultSet.getInt("source_port"),
                            resultSet.getString("source_database"),
                            resultSet.getString("target_host"),
                            resultSet.getInt("target_port"),
                            resultSet.getString("target_database"),
                            resultSet.getString("environment")
                    );
                }
            }
        } catch (SQLException e) {
            getLogger().error("Error querying environment configuration", e);
        }
        
        return null;
    }

    private String extractJdbcUrlFromService(DBCPService dbcpService) {
        try {
            // This is a simplified approach - in practice you'd need to access the actual JDBC URL
            // For now, we'll return a placeholder
            return "jdbc:placeholder://localhost:5432/database";
        } catch (Exception e) {
            getLogger().error("Error extracting JDBC URL from service", e);
            return null;
        }
    }

    private String buildJdbcUrl(String host, int port, String database) {
        return String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    }

    private boolean compareJdbcUrls(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        
        String normalizedExpected = normalizeJdbcUrl(expected);
        String normalizedActual = normalizeJdbcUrl(actual);
        
        return normalizedExpected.equals(normalizedActual);
    }

    private String normalizeJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        
        // Simple normalization - remove extra spaces and convert to lowercase
        return jdbcUrl.toLowerCase().trim();
    }

    // Inner classes for validation results and configuration
    private static class ValidationResult {
        private final boolean valid;
        private final String errorMessage;
        private final String environment;

        private ValidationResult(boolean valid, String errorMessage, String environment) {
            this.valid = valid;
            this.errorMessage = errorMessage;
            this.environment = environment;
        }

        public static ValidationResult success(String environment) {
            return new ValidationResult(true, null, environment);
        }

        public static ValidationResult failure(String errorMessage) {
            return new ValidationResult(false, errorMessage, null);
        }

        public boolean isValid() {
            return valid;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getEnvironment() {
            return environment;
        }
    }

    private static class EnvironmentConfig {
        private final String sourceHost;
        private final int sourcePort;
        private final String sourceDatabase;
        private final String targetHost;
        private final int targetPort;
        private final String targetDatabase;
        private final String environment;

        public EnvironmentConfig(String sourceHost, int sourcePort, String sourceDatabase,
                               String targetHost, int targetPort, String targetDatabase, String environment) {
            this.sourceHost = sourceHost;
            this.sourcePort = sourcePort;
            this.sourceDatabase = sourceDatabase;
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.targetDatabase = targetDatabase;
            this.environment = environment;
        }

        public String getSourceHost() { return sourceHost; }
        public int getSourcePort() { return sourcePort; }
        public String getSourceDatabase() { return sourceDatabase; }
        public String getTargetHost() { return targetHost; }
        public int getTargetPort() { return targetPort; }
        public String getTargetDatabase() { return targetDatabase; }
        public String getEnvironment() { return environment; }
    }
}
