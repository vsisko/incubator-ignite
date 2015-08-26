/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import net.sf.json.*;
import net.sf.json.processors.*;

import java.util.*;

/**
 * Jetty protocol json configuration.
 */
public class GridJettyJsonConfig extends JsonConfig {
    /**
     * Constructs default jetty json config.
     */
    public GridJettyJsonConfig() {
        registerJsonValueProcessor(UUID.class, new UUIDToStringJsonProcessor());
        registerJsonValueProcessor(Date.class, new DateToStringJsonProcessor());
        registerJsonValueProcessor(java.sql.Date.class, new DateToStringJsonProcessor());
    }

    /**
     * Helper class for simple to-string conversion for the beans.
     */
    private static class UUIDToStringJsonProcessor implements JsonValueProcessor {
        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val instanceof UUID)
                return val.toString();

            if (val instanceof UUID[]) {
                UUID[] uuids = (UUID[])val;

                String[] result = new String[uuids.length];

                for (int i = 0; i < uuids.length; i++)
                    result[i] = uuids[i] == null ? null : uuids[i].toString();

                return result;
            }

            throw new UnsupportedOperationException("Serialize array to string is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return val == null ? null : val.toString();
        }
    }

    /**
     * Helper class for simple to-string conversion for the beans.
     */
    private static class DateToStringJsonProcessor implements JsonValueProcessor {
        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            if (val instanceof Date)
                return val.toString();

            if (val instanceof Date[]) {
                Date[] dates = (Date[])val;

                String[] result = new String[dates.length];

                for (int i = 0; i < dates.length; i++)
                    result[i] = dates[i] == null ? null : dates[i].toString();

                return result;
            }

            throw new UnsupportedOperationException("Serialize array to string is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonConfig) {
            return val == null ? null : val.toString();
        }
    }
}
