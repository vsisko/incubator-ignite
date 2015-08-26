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

var flash = require('connect-flash');
var express = require('express');
var compress = require('compression');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var session = require('express-session');
var mongoStore = require('connect-mongo')(session);

var publicRoutes = require('./routes/public');
var notebooksRoutes = require('./routes/notebooks');
var clustersRouter = require('./routes/clusters');
var cachesRouter = require('./routes/caches');
var metadataRouter = require('./routes/metadata');
var presetsRouter = require('./routes/presets');
var summary = require('./routes/summary');
var adminRouter = require('./routes/admin');
var profileRouter = require('./routes/profile');
var sqlRouter = require('./routes/sql');
var agentRouter = require('./routes/agent');

var passport = require('passport');

var db = require('./db');

var app = express();

app.use(compress());

// Views engine setup.
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// Site favicon.
app.use(favicon(__dirname + '/public/favicon.ico'));

app.use(logger('dev'));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

app.use(require('node-sass-middleware')({
    /* Options */
    src: path.join(__dirname, 'public'),
    dest: path.join(__dirname, 'public'),
    debug: true,
    outputStyle: 'nested'
}));

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'controllers')));
app.use(express.static(path.join(__dirname, 'helpers')));
app.use(express.static(path.join(__dirname, 'routes/generator')));

app.use(cookieParser('keyboard cat'));

app.use(session({
    secret: 'keyboard cat',
    resave: false,
    saveUninitialized: true,
    store: new mongoStore({
        mongooseConnection: db.mongoose.connection
    })
}));

app.use(flash());

app.use(passport.initialize());
app.use(passport.session());

passport.serializeUser(db.Account.serializeUser());
passport.deserializeUser(db.Account.deserializeUser());

passport.use(db.Account.createStrategy());

var mustAuthenticated = function (req, res, next) {
    req.isAuthenticated() ? next() : res.redirect('/');
};

var adminOnly = function(req, res, next) {
    req.isAuthenticated() && req.user.admin ? next() : res.sendStatus(403);
};

app.all('/configuration/*', mustAuthenticated);

app.all('*', function(req, res, next) {
    var becomeUsed = req.session.viewedUser && req.user.admin;

    if (req.url.lastIndexOf('/reset', 0) === 0) {
        res.locals.user = null;
        res.locals.becomeUsed = false;
    }
    else {
        res.locals.user = becomeUsed ? req.session.viewedUser : req.user;
        res.locals.becomeUsed = becomeUsed;
    }

    req.currentUserId = function() {
        if (!req.user)
            return null;

        if (req.session.viewedUser && req.user.admin)
            return req.session.viewedUser._id;

        return req.user._id;
    };

    next();
});

app.use('/', publicRoutes);
app.use('/admin', mustAuthenticated, adminOnly, adminRouter);
app.use('/profile', mustAuthenticated, profileRouter);

app.use('/configuration/clusters', clustersRouter);
app.use('/configuration/caches', cachesRouter);
app.use('/configuration/metadata', metadataRouter);
app.use('/configuration/presets', presetsRouter);
app.use('/configuration/summary', summary);

app.use('/notebooks', mustAuthenticated, notebooksRoutes);
app.use('/sql', mustAuthenticated, sqlRouter);

app.use('/agent', mustAuthenticated, agentRouter);

// Catch 404 and forward to error handler.
app.use(function (req, res, next) {
    var err = new Error('Not Found: ' + req.originalUrl);
    err.status = 404;
    next(err);
});

// Error handlers.

// Development error handler: will print stacktrace.
if (app.get('env') === 'development') {
    app.use(function (err, req, res) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// Production error handler: no stacktraces leaked to user.
app.use(function (err, req, res) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});

module.exports = app;
