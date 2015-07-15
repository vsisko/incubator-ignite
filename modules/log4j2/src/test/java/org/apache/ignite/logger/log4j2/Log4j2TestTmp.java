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

package org.apache.ignite.logger.log4j2;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;

/**
 * TODO: Add class description.
 */
public class Log4j2TestTmp {
    public static void main(String[] args) throws Exception {
//        URL url = U.resolveIgniteUrl("config/ignite-log4j2.xml");
//
//        System.out.println(url);
//
//        Configurator.initialize("test logger", url.toString());
//
//        LogManager.getLogger("test logger").info("******************************1");
//
//        ThreadContext.put("nodeId", "12345");
//
//        LogManager.getLogger("test logger").info("******************************2");

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setGridLogger(new Log4J2Logger("config/ignite-log4j2.xml"));

        try (Ignite ignite = Ignition.start(cfg.setGridName("grid1"))) {
            ignite.log().info("****** smf 1 ********");
            try (Ignite ignite2 = Ignition.start(cfg.setGridName("grid2"))) {
                ignite.log().info("****** smf 2 ********");
                ignite2.log().info("****** smf 3 ********");
            }
        }
    }
}
