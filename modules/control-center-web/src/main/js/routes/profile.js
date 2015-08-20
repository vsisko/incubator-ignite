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
var db = require('../db');

/**
 * Get user profile page.
 */
router.get('/', function (req, res) {
    var user_id = req.currentUserId();

    db.Account.findById(user_id, function (err) {
        if (err)
            return res.status(500).send(err.message);

        res.render('settings/profile');
    });
});

/**
 * Save user profile.
 */
router.post('/save', function (req, res) {
    var params = req.body;

    if (params.newPassword) {
        var newPassword = params.newPassword;

        if (!newPassword || newPassword.length == 0)
            return res.status(500).send('Wrong value for new password');

        db.Account.findById(params._id, function (err, user) {
            if (err)
                return res.status(500).send(err);

            user.setPassword(newPassword, function (err, updatedUser) {
                if (err)
                    return res.status(500).send(err.message);

                if (params.userName)
                    updatedUser.username = params.userName;

                if (params.email)
                    updatedUser.email = params.email;

                updatedUser.save(function (err) {
                    if (err)
                        return res.status(500).send(err.message);

                    res.json(user);
                });
            });
        });
    }
    else if (params.userName || params.email) {
        var upd = {};

        if (params.userName)
            upd.username = params.userName;

        if (params.email)
            upd.email = params.email;

        db.Account.findByIdAndUpdate(params._id, upd, {new: true}, function (err, val) {
            if (err)
                return res.status(500).send(err.message);

            res.json(val);
        })
    }
});

module.exports = router;
