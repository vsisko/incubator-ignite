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

import com.google.gson.*;
import org.apache.ignite.internal.processors.rest.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Ignite field name strategy.
 */
public class IgniteFieldNamingStrategy implements FieldNamingStrategy {
    /** */
    private static final Map<Field, String> FIELD_NAMES = new HashMap<>();

    static {
        addField(GridRestResponse.class, "obj", "response");
        addField(GridRestResponse.class, "err", "error");
        addField(GridRestResponse.class, "sesTokStr", "sessionToken");
    }

    /**
     * @param cls Class.
     * @param fldName Fld name.
     * @param propName Property name.
     */
    private static void addField(Class<?> cls, String fldName, String propName) {
        try {
            Field field = cls.getDeclaredField(fldName);

            // assert method exists.
            cls.getDeclaredMethod("get" + Character.toUpperCase(propName.charAt(0)) + propName.substring(1));

            String old = FIELD_NAMES.put(field, propName);

            assert old == null;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String translateName(Field f) {
        String name = FIELD_NAMES.get(f);

        return name == null ? f.getName() : name;
    }
}
