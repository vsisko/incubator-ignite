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

controlCenterModule.controller('sqlController', ['$scope', '$controller', '$http', '$common',
    function ($scope, $controller, $http, $common) {
    // Initialize the super class and extend it.
    angular.extend(this, $controller('agent-download', {$scope: $scope}));

    $scope.joinTip = $common.joinTip;

    $scope.pageSizes = [50, 100, 200, 400, 800, 1000];

    $scope.modes = [
        {value: 'PARTITIONED', label: 'PARTITIONED'},
        {value: 'REPLICATED', label: 'REPLICATED'},
        {value: 'LOCAL', label: 'LOCAL'}
    ];

    var loadNotebook = function() {
        $http.post('/notebooks/get', {noteId: $scope.noteId})
            .success(function (notebook) {
                $scope.notebook = notebook;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    loadNotebook();

    $scope.saveNotebook = function(notebook) {
        $scope.notebook_edit = false;

        $http.post('/notebooks/save', notebook)
            .success(function () {
                var idx = _.findIndex($scope.$root.notebooks, function (item) {
                    return item._id == notebook._id;
                });

                if (idx >= 0) {
                    $scope.$root.notebooks[idx].name = $scope.notebook.name;

                    $scope.$root.rebuildDropdown();
                }
            })
            .error(function (errMsg) {
                $common.showError(errMsg);

                loadNotebook();
            });
    };

    $scope.addParagraph = function(notebook) {
        notebook.paragraphs.push({});
    };

    $scope.tabs = [];

    $scope.addTab = function() {
        var tab = {query: "", pageSize: $scope.pageSizes[0]};

        if ($scope.caches.length > 0)
            tab.selectedItem = $scope.caches[0];

        $scope.tabs.push(tab);
    };

    $scope.removeTab = function(idx) {
        $scope.tabs.splice(idx, 1);
    };

    $http.get('/models/sql.json')
        .success(function (data) {
            $scope.screenTip = data.screenTip;
            $scope.missingClientTip = data.missingClientTip;
        })
        .error(function (errMsg) {
            $common.showError(errMsg);
        });

    $scope.caches = undefined;

    $http.post('/agent/topology')
        .success(function (clusters) {
            var node = clusters[0];

            $scope.caches = node.caches;

            if ($scope.tabs.length == 0)
                $scope.addTab();
        })
        .error(function (err, status) {
            $scope.caches = undefined;

            if (status == 503)
                $scope.showDownloadAgent();
            else
                $common.showError('Receive agent error: ' + err);
        });

    $scope.execute = function(tab) {
        $http.post('/agent/query', {query: tab.query, pageSize: tab.pageSize, cacheName: tab.selectedItem.name})
            .success(function (res) {
                tab.meta = [];

                if (res.meta)
                    tab.meta = res.meta;

                tab.page = 1;

                tab.total = 0;

                tab.queryId = res.queryId;

                tab.rows = res.rows;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.explain = function(tab) {
        $http.post('/agent/query', {query: 'EXPLAIN ' + tab.query, pageSize: tab.pageSize, cacheName: tab.selectedItem.name})
            .success(function (res) {
                tab.meta = [];

                if (res.meta)
                    tab.meta = res.meta;

                tab.page = 1;

                tab.total = 0;

                tab.queryId = res.queryId;

                tab.rows = res.rows;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.nextPage = function(tab) {
        $http.post('/agent/query/fetch', {queryId: tab.queryId, pageSize: tab.pageSize, cacheName: tab.selectedItem.name})
            .success(function (res) {
                tab.page++;

                tab.total += tab.rows.length;

                tab.rows = res.rows;

                if (res.last)
                    delete tab.queryId;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.getter = function (value) {
        return value;
    }
}]);
