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
    $scope.agentGoal = 'execute sql statements';
    $scope.agentTestDriveOption = '--test-sql';

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

                $scope.notebook_name = notebook.name;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    loadNotebook();

    $scope.renameNotebook = function(name) {
        if ($scope.notebook.name != name) {
            $scope.notebook.name = name;

            $http.post('/notebooks/save', $scope.notebook)
                .success(function () {
                    var idx = _.findIndex($scope.$root.notebooks, function (item) {
                        return item._id == $scope.notebook._id;
                    });

                    if (idx >= 0) {
                        $scope.$root.notebooks[idx].name = name;

                        $scope.$root.rebuildDropdown();
                    }

                    $scope.notebook.edit = false;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });
        }
        else
            $scope.notebook.edit = false
    };

    $scope.renameParagraph = function(paragraph, newName) {
        if (paragraph.name != newName) {
            paragraph.name = newName;

            $http.post('/notebooks/save', $scope.notebook)
                .success(function () {
                    paragraph.edit = false;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });
        }
        else
            paragraph.edit = false
    };

    $scope.addParagraph = function() {
        if (!$scope.notebook.paragraphs)
            $scope.notebook.paragraphs = [];

        var sz = $scope.notebook.paragraphs.length;

        var paragraph = {name: 'Query' + (sz ==0 ? '' : sz), editor: true, query: '', pageSize: $scope.pageSizes[0]};

        if ($scope.caches.length > 0)
            paragraph.cache = $scope.caches[0];

        $scope.notebook.paragraphs.push(paragraph);
    };

    $scope.removeParagraph = function(idx) {
        $scope.notebook.splice(idx, 1);
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

            if (!$scope.notebook.paragraphs || $scope.notebook.paragraphs.length == 0)
                $scope.addParagraph();
        })
        .error(function (err, status) {
            $scope.caches = undefined;

            if (status == 503)
                $scope.showDownloadAgent();
            else
                $common.showError('Receive agent error: ' + err);
        });

    var _appendOnLast = function(item) {
        var idx = _.findIndex($scope.notebook.paragraphs, function (paragraph) {
            return paragraph == item;
        });

        if ($scope.notebook.paragraphs.length == (idx + 1))
            $scope.addParagraph();
    };

    var _processQueryResult = function(item) {
        return function(res) {
            item.meta = [];

            if (res.meta)
                item.meta = res.meta;

            item.page = 1;

            item.total = 0;

            item.queryId = res.queryId;

            item.rows = res.rows;

            item.result = 'table';
        }
    };

    $scope.execute = function(item) {
        _appendOnLast(item);

        $http.post('/agent/query', {query: item.query, pageSize: item.pageSize, cacheName: item.cache.name})
            .success(_processQueryResult(item))
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.explain = function(item) {
        _appendOnLast(item);

        $http.post('/agent/query', {query: 'EXPLAIN ' + item.query, pageSize: item.pageSize, cacheName: item.cache.name})
            .success(_processQueryResult(item))
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.scan = function(item) {
        _appendOnLast(item);

        $http.post('/agent/scan', {pageSize: item.pageSize, cacheName: item.cache.name})
            .success(_processQueryResult(item))
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.nextPage = function(item) {
        $http.post('/agent/query/fetch', {queryId: item.queryId, pageSize: item.pageSize, cacheName: item.cache.name})
            .success(function (res) {
                item.page++;

                item.total += item.rows.length;

                item.rows = res.rows;

                if (res.last)
                    delete item.queryId;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.columnToolTip = function(col) {
        var res = [];

        if (col.schemaName)
            res.push(col.schemaName);
        if (col.typeName)
            res.push(col.typeName);

        res.push(col.fieldName);

        return res.join(".");
    };

    $scope.resultMode = function(paragraph, type) {
        return (paragraph.result === type);
    };

    $scope.getter = function (value) {
        return value;
    }
}]);
