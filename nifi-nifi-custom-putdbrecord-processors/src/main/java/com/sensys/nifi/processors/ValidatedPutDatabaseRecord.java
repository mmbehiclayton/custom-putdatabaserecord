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
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import com.sensys.nifi.processors.PutDatabaseRecord;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

/**
 * Custom PutDatabaseRecord processor with environment validation.
 * Extends the standard PutDatabaseRecord processor to add environment validation
 * capabilities while retaining all original functionality.
 * 
 * Validates that the configured controller services (source and target) match the expected
 * environment configuration before upserting records to the target database.
 * 
 * Uses operation_id from FlowFile attributes to look up environment configuration
 * from sse_engine database.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"database", "put", "upsert", "validation", "environment", "sensys", "sse"})
@CapabilityDescription("Puts records into a target database with environment validation. " +
    "Extends the standard PutDatabaseRecord processor to validate that controller services " +
    "match the expected environment configuration by querying the SSE Engine database " +
    "using operation_id from FlowFile attributes. Retains all original PutDatabaseRecord functionality.")
@ReadsAttribute(attribute = "operation.id", description = "Operation ID used to look up environment configuration")
@WritesAttribute(attribute = "validation.passed", description = "true if validation passed")
@WritesAttribute(attribute = "validation.environment.id", description = "Environment ID used for validation")
@WritesAttribute(attribute = "validation.environment.name", description = "Environment name")
@WritesAttribute(attribute = "validation.timestamp", description = "Validation timestamp")
@WritesAttribute(attribute = "validation.error", description = "Validation error message if failed")
@UseCase(description = "Insert records into a database with environment validation")
public class ValidatedPutDatabaseRecord extends PutDatabaseRecord {
    
    // Additional relationship for validation failures
    public static final Relationship REL_VALIDATION_FAILED = new Relationship.Builder()
            .name("validation_failed")
            .description("FlowFiles that fail environment validation are routed to this relationship")
            .build();

    // --- Environment Validation Properties ---
    public static final PropertyDescriptor ENABLE_VALIDATION = new Builder()
            .name("enable-validation")
            .displayName("Enable Validation")
            .description("Enable or disable environment validation before executing database operations.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor OPERATION_ID = new Builder()
            .name("operation-id")
            .displayName("Operation ID")
            .description("FlowFile attribute name/value used to look up environment configuration in the SSE Engine database.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${operation.id}")
            .build();

    public static final PropertyDescriptor ENGINE_DBCP_SERVICE = new Builder()
            .name("engine-dbcp-service")
            .displayName("SSE Engine Database Connection Pool")
            .description("DBCP Controller Service for connecting to the SSE Engine (metadata) database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SOURCE_DBCP_SERVICE = new Builder()
            .name("source-dbcp-service")
            .displayName("Source Database Connection Pool")
            .description("DBCP Controller Service for the SOURCE database to validate against environment configuration.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TARGET_DBCP_SERVICE = new Builder()
            .name("target-dbcp-service")
            .displayName("Target Database Connection Pool (Validation)")
            .description("DBCP Controller Service for the TARGET database to validate against environment configuration. This does not replace the main Database Connection Pooling Service used for writes.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor VALIDATION_MODE = new Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("STRICT routes to 'validation_failed' on mismatch; WARNING logs and proceeds.")
            .required(true)
            .allowableValues("STRICT", "WARNING")
            .defaultValue("STRICT")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> enhanced = new ArrayList<>(super.getSupportedPropertyDescriptors());
        // Add validation-related properties at the beginning for visibility
        enhanced.add(0, ENABLE_VALIDATION);
        enhanced.add(1, OPERATION_ID);
        enhanced.add(2, ENGINE_DBCP_SERVICE);
        enhanced.add(3, SOURCE_DBCP_SERVICE);
        enhanced.add(4, TARGET_DBCP_SERVICE);
        enhanced.add(5, VALIDATION_MODE);
        return enhanced;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>(super.getRelationships());
        rels.add(REL_VALIDATION_FAILED);
        return rels;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Environment validation before DB operation (if enabled)
        final Boolean validationEnabled = context.getProperty(ENABLE_VALIDATION).asBoolean();
        if (validationEnabled != null && validationEnabled) {
            try {
                final ValidationResultEx validation = performEnvironmentValidation(context, flowFile);
                if (!validation.valid) {
                    final String mode = context.getProperty(VALIDATION_MODE).getValue();
                    if ("STRICT".equals(mode)) {
                        flowFile = session.putAttribute(flowFile, "validation.passed", "false");
                        flowFile = session.putAttribute(flowFile, "validation.error", validation.errorMessage);
                        flowFile = session.putAttribute(flowFile, "validation.timestamp", String.valueOf(System.currentTimeMillis()));
                        getLogger().error("Environment validation failed for operation_id={}: {}", validation.operationId, validation.errorMessage);
                        session.transfer(flowFile, REL_VALIDATION_FAILED);
                        return;
                    } else {
                        getLogger().warn("Environment validation warning for operation_id={}: {} - proceeding", validation.operationId, validation.errorMessage);
                    }
                } else {
                    flowFile = session.putAttribute(flowFile, "validation.passed", "true");
                    if (validation.environmentId != null) {
                        flowFile = session.putAttribute(flowFile, "validation.environment.id", String.valueOf(validation.environmentId));
                    }
                    if (validation.environmentName != null) {
                        flowFile = session.putAttribute(flowFile, "validation.environment.name", validation.environmentName);
                    }
                    flowFile = session.putAttribute(flowFile, "validation.timestamp", String.valueOf(System.currentTimeMillis()));
                }
            } catch (final Exception e) {
                getLogger().error("Error during environment validation: {}", e.getMessage(), e);
                flowFile = session.putAttribute(flowFile, "validation.error", "Validation error: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        // Proceed with standard PutDatabaseRecord operation
        super.onTrigger(context, session);
    }

    // ------------------- Validation Helpers -------------------

    private record ValidationResultEx(boolean valid, String operationId, Long environmentId, String environmentName, String errorMessage) {}

    private record EnvironmentConfig(Long environmentId,
                                     String environmentName,
                                     String environmentStatus,
                                     String sourceDbType,
                                     String sourceHost,
                                     Integer sourcePort,
                                     String sourceDatabase,
                                     String sourceName,
                                     String targetDbType,
                                     String targetHost,
                                     Integer targetPort,
                                     String targetDatabase,
                                     String targetName) {}

    private ValidationResultEx performEnvironmentValidation(final ProcessContext context, final FlowFile flowFile) throws SQLException {
        final String operationId = context.getProperty(OPERATION_ID).evaluateAttributeExpressions(flowFile).getValue();
        if (operationId == null || operationId.trim().isEmpty()) {
            return new ValidationResultEx(false, operationId, null, null, "operation_id is empty or not set");
        }

        getLogger().debug("Validating environment for operation_id: {}", operationId);

        final DBCPService engineDbcp = context.getProperty(ENGINE_DBCP_SERVICE).asControllerService(DBCPService.class);
        final DBCPService sourceDbcp = context.getProperty(SOURCE_DBCP_SERVICE).asControllerService(DBCPService.class);
        final DBCPService targetDbcp = context.getProperty(TARGET_DBCP_SERVICE).asControllerService(DBCPService.class);

        final EnvironmentConfig envConfig = queryEnvironmentConfig(engineDbcp, operationId);
        if (envConfig == null) {
            return new ValidationResultEx(false, operationId, null, null, "No environment configuration found for operation_id: " + operationId);
        }

        final String sourceJdbcUrl = extractJdbcUrlFromService(sourceDbcp);
        final String targetJdbcUrl = extractJdbcUrlFromService(targetDbcp);

        final String expectedSourceUrl = buildJdbcUrl(envConfig.sourceDbType, envConfig.sourceHost, envConfig.sourcePort, envConfig.sourceDatabase);
        final String expectedTargetUrl = buildJdbcUrl(envConfig.targetDbType, envConfig.targetHost, envConfig.targetPort, envConfig.targetDatabase);

        getLogger().debug("Expected Source URL: {}", expectedSourceUrl);
        getLogger().debug("Actual Source URL: {}", sourceJdbcUrl);
        getLogger().debug("Expected Target URL: {}", expectedTargetUrl);
        getLogger().debug("Actual Target URL: {}", targetJdbcUrl);

        final boolean sourceMatches = compareJdbcUrls(sourceJdbcUrl, expectedSourceUrl);
        final boolean targetMatches = compareJdbcUrls(targetJdbcUrl, expectedTargetUrl);

        if (!sourceMatches || !targetMatches) {
            final String errorMsg = String.format(
                    "Controller service mismatch for environment '%s' (ID: %d). Source match: %s, Target match: %s. Expected: Source=%s, Target=%s. Actual: Source=%s, Target=%s",
                    envConfig.environmentName,
                    envConfig.environmentId,
                    sourceMatches,
                    targetMatches,
                    expectedSourceUrl,
                    expectedTargetUrl,
                    sourceJdbcUrl,
                    targetJdbcUrl
            );
            return new ValidationResultEx(false, operationId, null, null, errorMsg);
        }

        return new ValidationResultEx(true, operationId, envConfig.environmentId, envConfig.environmentName, null);
    }

    private EnvironmentConfig queryEnvironmentConfig(final DBCPService engineDbcp, final String operationId) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = engineDbcp.getConnection();
            final String sql = """
                SELECT DISTINCT
                    o.id as operation_id,
                    o.environment_id,
                    e.name as environment_name,
                    e.status as environment_status,
                    sc.database_type as source_db_type,
                    sc.host as source_host,
                    sc.port as source_port,
                    sc.db_name as source_database,
                    sc.name as source_name,
                    tc.database_type as target_db_type,
                    tc.host as target_host,
                    tc.port as target_port,
                    tc.db_name as target_database,
                    tc.name as target_name
                FROM data_migration_records dmr
                INNER JOIN operations o ON dmr.operation_id = o.id
                INNER JOIN environment_configurations e ON o.environment_id = e.id
                INNER JOIN database_configurations sc ON e.source_config_id = sc.id AND sc.deleted = FALSE
                INNER JOIN database_configurations tc ON e.target_config_id = tc.id AND tc.deleted = FALSE
                WHERE dmr.operation_id = ?
                LIMIT 1
                """;
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, operationId);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return new EnvironmentConfig(
                    rs.getLong("environment_id"),
                    rs.getString("environment_name"),
                    rs.getString("environment_status"),
                    rs.getString("source_db_type"),
                    rs.getString("source_host"),
                    rs.getInt("source_port"),
                    rs.getString("source_database"),
                    rs.getString("source_name"),
                    rs.getString("target_db_type"),
                    rs.getString("target_host"),
                    rs.getInt("target_port"),
                    rs.getString("target_database"),
                    rs.getString("target_name")
            );
        } finally {
            if (rs != null) { try { rs.close(); } catch (SQLException ignore) {} }
            if (stmt != null) { try { stmt.close(); } catch (SQLException ignore) {} }
            if (conn != null) { try { conn.close(); } catch (SQLException ignore) {} }
        }
    }

    private String extractJdbcUrlFromService(final DBCPService dbcpService) throws SQLException {
        Connection conn = null;
        try {
            conn = dbcpService.getConnection();
            final DatabaseMetaData metadata = conn.getMetaData();
            return metadata != null ? metadata.getURL() : null;
        } finally {
            if (conn != null) { try { conn.close(); } catch (SQLException ignore) {} }
        }
    }

    private String buildJdbcUrl(final String dbType, final String host, final int port, final String database) {
        final String base = switch (dbType.toLowerCase()) {
            case "oracle" -> "jdbc:oracle:thin:@%s:%d/%s";
            case "postgresql" -> "jdbc:postgresql://%s:%d/%s";
            case "mysql" -> "jdbc:mysql://%s:%d/%s";
            case "sqlserver" -> "jdbc:sqlserver://%s:%d;databaseName=%s";
            default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
        };
        return String.format(base, host, port, database);
    }

    private boolean compareJdbcUrls(final String url1, final String url2) {
        if (url1 == null || url2 == null) {
            return false;
        }
        return normalizeJdbcUrl(url1).equalsIgnoreCase(normalizeJdbcUrl(url2));
    }

    private String normalizeJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return "";
        }
        final int queryIndex = jdbcUrl.indexOf('?');
        if (queryIndex > 0) {
            jdbcUrl = jdbcUrl.substring(0, queryIndex);
        }
        jdbcUrl = jdbcUrl.replaceAll("/+$", "");
        return jdbcUrl.trim();
    }
}