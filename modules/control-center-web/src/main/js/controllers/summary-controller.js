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

// Controller for Summary screen.
controlCenterModule.controller('summaryController', ['$scope', '$http', '$common', function ($scope, $http, $common) {
    $scope.joinTip = $common.joinTip;
    $scope.getModel = $common.getModel;

    $scope.javaClassItems = [
        {label: 'snippet', value: 1},
        {label: 'factory class', value: 2}
    ];

    $scope.evictionPolicies = [
        {value: 'LRU', label: 'LRU'},
        {value: 'RND', label: 'Random'},
        {value: 'FIFO', label: 'FIFO'},
        {value: 'SORTED', label: 'Sorted'},
        {value: undefined, label: 'Not set'}
    ];

    $scope.oss = ['debian:8', 'ubuntu:14.10'];

    $scope.configServer = {javaClassServer: 1, os: undefined};

    $scope.backupItem = {javaClassClient: 1};

    $http.get('/models/summary.json')
        .success(function (data) {
            $scope.screenTip = data.screenTip;
            $scope.clientFields = data.clientFields;
        })
        .error(function (errMsg) {
            $common.showError(errMsg);
        });

    $scope.clusters = [];

    $scope.aceInit = function (editor) {
        editor.setReadOnly(true);
        editor.setOption('highlightActiveLine', false);
        editor.$blockScrolling = Infinity;

        var renderer = editor.renderer;

        renderer.setHighlightGutterLine(false);
        renderer.setShowPrintMargin(false);
        renderer.setOption('fontSize', '14px');

        editor.setTheme('ace/theme/chrome');
    };

    $scope.generateJavaServer = function () {
        $scope.javaServer = $generatorJava.cluster($scope.selectedItem, $scope.configServer.javaClassServer === 2);
    };

    $scope.$watch('configServer.javaClassServer', $scope.generateJavaServer, true);

    $scope.generateDockerServer = function() {
        var os = $scope.configServer.os ? $scope.configServer.os : $scope.oss[0];

        $scope.dockerServer = $generatorDocker.clusterDocker($scope.selectedItem, os);
    };

    $scope.$watch('configServer.os', $scope.generateDockerServer, true);

    $scope.generateClient = function () {
        $scope.xmlClient = $generatorXml.cluster($scope.selectedItem, $scope.backupItem.nearConfiguration);
        $scope.javaClient = $generatorJava.cluster($scope.selectedItem, $scope.backupItem.javaClassClient === 2,
            $scope.backupItem.nearConfiguration);
    };

    $scope.$watch('backupItem', $scope.generateClient, true);

    $scope.selectItem = function (cluster) {
        if (!cluster)
            return;

        $scope.selectedItem = cluster;

        $scope.xmlServer = $generatorXml.cluster(cluster);

        $scope.generateJavaServer();

        $scope.generateDockerServer();

        $scope.generateClient();
    };

    $scope.download = function () {
        $http.post('summary/download', {_id: $scope.selectedItem._id, os: $scope.os})
            .success(function (data) {
                var file = document.createElement('a');

                file.setAttribute('href', 'data:application/octet-stream;charset=utf-8,' + data);
                file.setAttribute('download', $scope.selectedItem.name + '-configuration.zip');

                file.style.display = 'none';

                document.body.appendChild(file);

                file.click();

                document.body.removeChild(file);
            })
            .error(function (errMsg) {
                $common.showError('Failed to generate zip: ' + errMsg);
            });
    };

    $http.post('clusters/list').success(function (data) {
        $scope.clusters = data.clusters;

        if ($scope.clusters.length > 0) {
            // Populate clusters with caches.
            _.forEach($scope.clusters, function (cluster) {
                cluster.caches = _.filter(data.caches, function (cache) {
                    return _.contains(cluster.caches, cache._id);
                });
            });

            var restoredId = sessionStorage.summarySelectedId;

            var selectIdx = 0;

            if (restoredId) {
                var idx = _.findIndex($scope.clusters, function (cluster) {
                    return cluster._id == restoredId;
                });

                if (idx >= 0)
                    selectIdx = idx;
                else
                    delete sessionStorage.summarySelectedId;
            }

            $scope.selectItem($scope.clusters[selectIdx]);

            $scope.$watch('selectedItem', function (val) {
                if (val)
                    sessionStorage.summarySelectedId = val._id;
            }, true);
        }
    });
}]);
