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

var router = require('express').Router();
var agentManager = require('../agents/agent-manager');

var apacheIgnite = require('apache-ignite');
var SqlFieldsQuery = apacheIgnite.SqlFieldsQuery;

/* Get grid topology. */
router.post('/topology', function (req, res) {
    var client = agentManager.getAgentManager().findClient(req.currentUserId());

    if (!client)
        return res.status(500).send("Client not found");

    client.ignite().cluster().then(function (clusters) {
        res.json(clusters.map(function (cluster) {
            var caches = Object.keys(cluster._caches).map(function (key) {
                return {"name": key, "mode": cluster._caches[key]}
            });

            return {nodeId: cluster._nodeId, caches: caches};
        }));
    }, function (err) {
        res.status(500).send(err);
    });
});

/* Execute query. */
router.post('/query', function (req, res) {
    var client = agentManager.getAgentManager().findClient(req.currentUserId());

    if (!client)
        return res.status(500).send("Client not found");

    // Create sql query.
    var qry = new SqlFieldsQuery(req.body.query);

    // Set page size for query.
    qry.setPageSize(req.body.pageSize);

    // Get query cursor.
    client.ignite().cache(req.body.cacheName).query(qry).nextPage().then(function (cursor) {
        res.json({meta: cursor.fieldsMetadata(), rows: cursor.page(), queryId: cursor.queryId()});
    }, function (err) {
        res.status(500).send(err);
    });
});

/* Get next query page. */
router.post('/next_page', function (req, res) {
    var client = agentManager.getAgentManager().findClient(req.currentUserId());

    if (!client)
        return res.status(500).send("Client not found");

    var cache = client.ignite().cache(req.body.cacheName);

    var cmd = cache._createCommand("qryfetch").addParam("qryId", req.body.queryId).
        addParam("pageSize", req.body.pageSize);

    cache.__createPromise(cmd).then(function (page) {
        res.json({rows: page["items"], last: page === null || page["last"]});
    }, function (err) {
        res.status(500).send(err);
    });
});

/* Get JDBC drivers list. */
router.post('/drivers', function (req, res) {
    res.json(['ojdbc6.jar', 'db2jcc4.jar', 'h2.jar']);

    //var client = agentManager.getAgentManager().findClient(req.currentUserId());
    //
    //if (!client)
    //    return res.status(500).send("Client not found");

});

/** Get database metadata. */
router.post('/metadata', function (req, res) {
    var tables = [];

    for (var i = 1; i < 17; i++) {
        tables.push({
            schemaName: 'Schema' + ((i / 5) + 1),
            use: true,
            tableName: 'Table' + i,
            keyClass: 'KeyClass' + i,
            valueClass: 'ValueClass' + i
        })
    }

    res.json(tables);

    //var client = agentManager.getAgentManager().findClient(req.currentUserId());
    //
    //if (!client)
    //    return res.status(500).send("Client not found");

});

module.exports = router;
