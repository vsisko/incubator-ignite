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

var _ = require('lodash');
var router = require('express').Router();
var db = require('../db');

/* GET clusters page. */
router.get('/', function (req, res) {
    res.render('configuration/clusters');
});

/**
 * Get spaces and clusters accessed for user account.
 *
 * @param req Request.
 * @param res Response.
 */
router.post('/list', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (err)
            return res.status(500).send(err.message);

        var space_ids = spaces.map(function (value) {
            return value._id;
        });

        db.Cache.find({space: {$in: space_ids}}, '_id name swapEnabled', function (err, caches) {
            if (err)
                return res.status(500).send(err);

            // Get all clusters for spaces.
            db.Cluster.find({space: {$in: space_ids}}).sort('name').exec(function (err, clusters) {
                if (err)
                    return res.status(500).send(err.message);

                // Remove deleted caches.
                _.forEach(clusters, function (cluster) {
                    cluster.caches = _.filter(cluster.caches, function (cacheId) {
                        return _.findIndex(caches, function (cache) {
                            return cache._id.equals(cacheId);
                        }) >= 0;
                    });
                });

                var cachesJson = caches.map(function (cache) {
                    return {value: cache._id, label: cache.name, swapEnabled: cache.swapEnabled};
                });

                res.json({spaces: spaces, caches: cachesJson, clusters: clusters});
            });
        });
    });
});

/**
 * Save cluster.
 */
router.post('/save', function (req, res) {
    var params = req.body;

    if (params._id)
        db.Cluster.update({_id: params._id}, params, {upsert: true}, function (err) {
            if (err)
                return res.status(500).send(err.message);

            res.send(params._id);
        });
    else {
        db.Cluster.findOne({space: params.space, name: params.name}, function (err, cluster) {
            if (err)
                return res.status(500).send(err.message);

            if (cluster)
                return res.status(500).send('Cluster with name: "' + cluster.name + '" already exist.');

            (new db.Cluster(params)).save(function (err, cluster) {
                if (err)
                    return res.status(500).send(err.message);

                res.send(cluster._id);
            });
        });
    }
});

/**
 * Remove cluster by ._id.
 */
router.post('/remove', function (req, res) {
    db.Cluster.remove(req.body, function (err) {
        if (err)
            return res.status(500).send(err.message);

        res.sendStatus(200);
    })
});

module.exports = router;
