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

var db = require('../db');

var router = require('express').Router();

var $generatorXml = require('./generator/generator-xml');
var $generatorJava = require('./generator/generator-java');
var $generatorDocker = require('./generator/generator-docker');
var $generatorProperties = require('./generator/generator-properties');

// GET template for summary tabs.
router.get('/summary-tabs', function (req, res) {
    res.render('configuration/summary-tabs', {});
});

/* GET summary page. */
router.get('/', function (req, res) {
    res.render('configuration/summary');
});

router.post('/download', function (req, res) {
    // Get cluster with all inner objects (caches, metadata).
    db.Cluster.findById(req.body._id).deepPopulate('caches caches.queryMetadata caches.storeMetadata').exec(function (err, cluster) {
        if (err)
            return res.status(500).send(err.message);

        if (!cluster)
            return res.sendStatus(404);

        var clientNearConfiguration = req.body.clientNearConfiguration;

        var archiver = require('archiver');

        // Creating archive.
        var zip = archiver('zip');

        zip.on('error', function (err) {
            res.status(500).send({error: err.message});
        });

        // On stream closed we can end the request.
        res.on('close', function () {
            return res.status(200).send('OK').end();
        });

        // Set the archive name.
        res.attachment(cluster.name + (clientNearConfiguration ? '-client' : '-server') + '-configuration.zip');

        // Send the file to the page output.
        zip.pipe(res);

        if (!clientNearConfiguration) {
            zip.append($generatorDocker.clusterDocker(cluster, req.body.os), {name: 'Dockerfile'});

            var props = $generatorProperties.dataSourcesProperties(cluster);

            if (props)
                zip.append(props, {name: 'secret.properties'});
        }

        zip.append($generatorXml.cluster(cluster, clientNearConfiguration), {name: cluster.name + '.xml'})
            .append($generatorJava.cluster(cluster, false, clientNearConfiguration),
                {name: cluster.name + '.snippet.java'})
            .append($generatorJava.cluster(cluster, true, clientNearConfiguration),
                {name: 'ConfigurationFactory.java'})
            .finalize();
    });
});

module.exports = router;
