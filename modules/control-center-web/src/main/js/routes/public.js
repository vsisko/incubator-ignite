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
var passport = require('passport');
var crypto = require('crypto');
var nodemailer = require('nodemailer');
var db = require('../db');

// GET dropdown-menu template.
router.get('/select', function (req, res) {
    res.render('templates/select', {});
});

// GET dropdown-menu template.
router.get('/popover', function (req, res) {
    res.render('templates/popover', {});
});

// GET dynamic tabs template.
router.get('/tab', function (req, res) {
    res.render('templates/tab', {});
});

// GET confirmation dialog.
router.get('/confirm', function (req, res) {
    res.render('templates/confirm', {});
});

// GET copy dialog.
router.get('/copy', function (req, res) {
    res.render('templates/copy', {});
});

/* GET login dialog. */
router.get('/login', function (req, res) {
    res.render('login');
});

/**
 * Register new account.
 */
router.post('/register', function (req, res) {
    db.Account.count(function (err, cnt) {
        if (err)
            return res.status(401).send(err.message);

        req.body.admin = cnt == 0;

        db.Account.register(new db.Account(req.body), req.body.password, function (err, account) {
            if (err)
                return res.status(401).send(err.message);

            if (!account)
                return res.status(500).send('Failed to create account.');

            new db.Space({name: 'Personal space', owner: account._id}).save();

            req.logIn(account, {}, function (err) {
                if (err)
                    return res.status(401).send(err.message);

                return res.redirect('/configuration/clusters');
            });
        });
    });
});

/**
 * Login in exist account.
 */
router.post('/login', function (req, res, next) {
    passport.authenticate('local', function (err, user) {
        if (err)
            return res.status(401).send(err.message);

        if (!user)
            return res.status(401).send('Invalid email or password');

        req.logIn(user, {}, function (err) {
            if (err)
                return res.status(401).send(err.message);

            res.redirect('/configuration/clusters');
        });
    })(req, res, next);
});

/**
 * Logout.
 */
router.get('/logout', function (req, res) {
    req.logout();

    res.redirect('/');
});

/**
 * Request for password reset and send e-mail to user with reset token. */
router.post('/request_password_reset', function(req, res) {
    var token = crypto.randomBytes(20).toString('hex');

    db.Account.findOne({ email: req.body.email }, function(err, user) {
        if (!user)
            return res.status(401).send('No account with that email address exists!');

        if (err)
            return res.status(401).send(err);

        user.resetPasswordToken = token;

        user.save(function(err) {
            if (err)
                return res.status(401).send(err);

            var transporter  = nodemailer.createTransport({
                service: 'gmail',
                auth: {
                    user: '!!! YOUR USERNAME !!!',
                    pass: '!!! YOUR PASSWORD !!!'
                }
            });

            var mailOptions = {
                from: '!!! YOUR USERNAME !!!',
                to: user.email,
                subject: 'Password Reset',
                text: 'You are receiving this because you (or someone else) have requested the reset of the password for your account.\n\n' +
                'Please click on the following link, or paste this into your browser to complete the process:\n\n' +
                'http://' + req.headers.host + '/reset/' + token + '\n\n' +
                'If you did not request this, please ignore this email and your password will remain unchanged.\n\n' +
                '--------------\n' +
                'Apache Ignite Web Control Center\n'
            };

            transporter.sendMail(mailOptions, function(err){
                if (err)
                    return res.status(401).send('Failed to send e-mail with reset link!');

                return res.status(403).send('An e-mail has been sent with further instructions.');
            });
        });
    });
});

router.get('/reset', function (req, res) {
    res.render('reset', {});
});

/* GET reset password page. */
router.get('/reset/:token', function (req, res) {
    var token = req.params.token;

    var data = {token: token};

    db.Account.findOne({resetPasswordToken: token}, function (err, user) {
        if (!user)
            data.error = 'Invalid token for password reset!';
        else if (err)
            data.error = err;

        res.render('reset', data);
    });
});

/**
 * Reset password with given token.
 */
router.post('/reset_password', function(req, res) {
    db.Account.findOne({ resetPasswordToken: req.body.token }, function(err, user) {
        if (!user)
            return res.status(500).send('Invalid token for password reset!');

        if (err)
            return res.status(500).send(err);

        user.setPassword(req.body.password, function (err, updatedUser) {
            if (err)
                return res.status(500).send(err.message);

            updatedUser.resetPasswordToken = undefined;

            updatedUser.save(function (err) {
                if (err)
                    return res.status(500).send(err.message);

                var transporter  = nodemailer.createTransport({
                    service: 'gmail',
                    auth: {
                        user: '!!! YOUR USERNAME !!!',
                        pass: '!!! YOUR PASSWORD !!!'
                    }
                });

                var mailOptions = {
                    from: '!!! YOUR USERNAME !!!',
                    to: user.email,
                    subject: 'Your password has been changed',
                    text: 'Hello,\n\n' +
                    'This is a confirmation that the password for your account ' + user.email + ' has just been changed.\n\n' +
                    'Now you can login: http://' + req.headers.host + '\n\n' +
                    '--------------\n' +
                    'Apache Ignite Web Control Center\n'
                };

                transporter.sendMail(mailOptions, function(err){
                    if (err)
                        return res.status(503).send('Password was changed, but failed to send confirmation e-mail!<br />' + err);

                    return res.status(200).send(user.email);
                });
            });
        });
    });
});

/* GET home page. */
router.get('/', function (req, res) {
    if (req.isAuthenticated())
        res.redirect('/configuration/clusters');
    else
        res.render('index');
});

module.exports = router;
