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

package org.apache.ignite.agent.handlers;

import org.apache.commons.codec.*;
import org.apache.http.*;
import org.apache.http.client.entity.*;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.*;
import org.apache.http.entity.*;
import org.apache.http.impl.client.*;
import org.apache.ignite.agent.*;
import org.apache.ignite.agent.remote.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;

/**
 * Executor for REST requests.
 */
public class RestExecutor {
    /** */
    private final AgentConfiguration cfg;

    /** */
    private CloseableHttpClient httpClient;

    /**
     * @param cfg Config.
     */
    public RestExecutor(AgentConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     *
     */
    public void start() {
        httpClient = HttpClientBuilder.create().build();
    }

    /**
     *
     */
    public void stop() throws IOException {
        if (httpClient != null)
            httpClient.close();
    }

    /**
     * @param path Path.
     * @param mtd Method.
     * @param params Params.
     * @param headers Headers.
     * @param body Body.
     */
    @Remote
    public RestResult executeRest(String path, Map<String, String> params, String mtd, Map<String, String> headers,
        String body) throws IOException, URISyntaxException {
        URIBuilder builder = new URIBuilder(cfg.nodeUri());

        if (path != null) {
            if (!path.startsWith("/") && !cfg.nodeUri().endsWith("/"))
                path = '/' +  path;

            builder.setPath(path);
        }

        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet())
                builder.addParameter(entry.getKey(), entry.getValue());
        }

        HttpRequestBase httpReq;

        if ("GET".equalsIgnoreCase(mtd))
            httpReq = new HttpGet(builder.build());
        else if ("POST".equalsIgnoreCase(mtd)) {
            HttpPost post;

            if (body == null) {
                List<NameValuePair> nvps = builder.getQueryParams();

                builder.clearParameters();

                post = new HttpPost(builder.build());

                if (!nvps.isEmpty())
                    post.setEntity(new UrlEncodedFormEntity(nvps));
            }
            else {
                post = new HttpPost(builder.build());

                post.setEntity(new StringEntity(body));
            }

            httpReq = post;
        }
        else
            throw new IOException("Unknown HTTP-method: " + mtd);

        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet())
                httpReq.addHeader(entry.getKey(), entry.getValue());
        }

        try (CloseableHttpResponse resp = httpClient.execute(httpReq)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            resp.getEntity().writeTo(out);

            Charset charset = Charsets.UTF_8;

            Header encodingHdr = resp.getEntity().getContentEncoding();

            if (encodingHdr != null) {
                String encoding = encodingHdr.getValue();

                charset = Charsets.toCharset(encoding);
            }

            return new RestResult(resp.getStatusLine().getStatusCode(), new String(out.toByteArray(), charset));
        }
    }

    /**
     * Request result.
     */
    public static class RestResult {
        /** Status code. */
        private int code;

        /** Message. */
        private String message;

        /**
         * @param code Code.
         * @param msg Message.
         */
        public RestResult(int code, String msg) {
            this.code = code;
            message = msg;
        }
    }
}
