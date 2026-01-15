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
package com.sensys.nifi.processors.db;

/**
 * Simplified NameNormalizerFactory for database operations
 */
public class NameNormalizerFactory {
    public static NameNormalizer getNormalizer(String strategy, String pattern) {
        // Simple implementation - return a basic normalizer
        return new SimpleNameNormalizer(strategy);
    }
    
    private static class SimpleNameNormalizer implements NameNormalizer {
        private final String strategy;
        
        public SimpleNameNormalizer(String strategy) {
            this.strategy = strategy;
        }
        
        @Override
        public String normalize(String name) {
            if (name == null) {
                return null;
            }
            
            if ("Remove Underscore".equals(strategy)) {
                return name.replace("_", "");
            } else if ("Lower Case".equals(strategy)) {
                return name.toLowerCase();
            } else if ("Upper Case".equals(strategy)) {
                return name.toUpperCase();
            }
            
            return name;
        }
    }
}

