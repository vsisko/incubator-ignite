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

// Controller for SQL notebook screen.
controlCenterModule.controller('sqlController', ['$scope', '$controller', '$http', '$common',
    function ($scope, $controller, $http, $common) {
    // Initialize the super class and extend it.
    angular.extend(this, $controller('agent-download', {$scope: $scope}));
    $scope.agentGoal = 'execute sql statements';
    $scope.agentTestDriveOption = '--test-sql';

    $scope.joinTip = $common.joinTip;

    $scope.pageSizes = [10, 25, 50];

    $scope.modes = [
        {value: 'PARTITIONED', label: 'PARTITIONED'},
        {value: 'REPLICATED', label: 'REPLICATED'},
        {value: 'LOCAL', label: 'LOCAL'}
    ];

    var loadNotebook = function () {
        $http.post('/notebooks/get', {noteId: $scope.noteId})
            .success(function (notebook) {
                $scope.notebook = notebook;

                $scope.notebook_name = notebook.name;

                $scope.notebook.activeIdx = [];

                if (!notebook.paragraphs || notebook.paragraphs.length == 0)
                    $scope.addParagraph();
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    loadNotebook();

    $scope.renameNotebook = function (name) {
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

                    $common.showInfo("Notebook successfully renamed.");
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });
        }
        else
            $scope.notebook.edit = false
    };

    $scope.saveNotebook = function () {
        $http.post('/notebooks/save', $scope.notebook)
            .success(function () {
                $common.showInfo("Notebook successfully saved.");
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.removeNotebook = function () {
        $http.post('/notebooks/remove', $scope.notebook)
            .success(function () {
                $common.showInfo("Notebook successfully removed.");
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.renameParagraph = function (paragraph, newName) {
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

    $scope.addParagraph = function () {
        if (!$scope.notebook.paragraphs)
            $scope.notebook.paragraphs = [];

        var sz = $scope.notebook.paragraphs.length;

        var paragraph = {name: 'Query' + (sz ==0 ? '' : sz), editor: true, query: '', pageSize: $scope.pageSizes[0]};

        if ($scope.caches && $scope.caches.length > 0)
            paragraph.cache = $scope.caches[0];

        $scope.notebook.activeIdx.push($scope.notebook.paragraphs.length);

        $scope.notebook.paragraphs.push(paragraph);
    };

    $scope.removeParagraph = function (paragraph) {
        var paragraph_idx = _.findIndex($scope.notebook.paragraphs, function (item) {
            return paragraph == item;
        });

        var panel_idx = _.findIndex($scope.notebook.activeIdx, function (item) {
            console.log(item);

            return paragraph_idx == item;
        });

        if (panel_idx >= 0)
            $scope.notebook.activeIdx.splice(panel_idx, 1);

        $scope.notebook.paragraphs.splice(paragraph_idx, 1);
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
        })
        .error(function (err, status) {
            $scope.caches = undefined;

            if (status == 503)
                $scope.showDownloadAgent();
            else
                $common.showError('Receive agent error: ' + err);
        });

    var _appendOnLast = function (item) {
        var idx = _.findIndex($scope.notebook.paragraphs, function (paragraph) {
            return paragraph == item;
        });

        if ($scope.notebook.paragraphs.length == (idx + 1))
            $scope.addParagraph();
    };

    var _processQueryResult = function (item) {
        return function (res) {
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

    $scope.execute = function (item) {
        _appendOnLast(item);

        $http.post('/agent/query', {query: item.query, pageSize: item.pageSize, cacheName: item.cache.name})
            .success(_processQueryResult(item))
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.explain = function (item) {
        _appendOnLast(item);

        $http.post('/agent/query', {
            query: 'EXPLAIN ' + item.query,
            pageSize: item.pageSize,
            cacheName: item.cache.name
        })
            .success(_processQueryResult(item))
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.scan = function (item) {
        _appendOnLast(item);

        $http.post('/agent/scan', {pageSize: item.pageSize, cacheName: item.cache.name})
            .success(_processQueryResult(item))
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.nextPage = function (item) {
        $http.post('/agent/query/fetch', {
            queryId: item.queryId,
            pageSize: item.pageSize,
            cacheName: item.cache.name
        })
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

    $scope.columnToolTip = function (col) {
        var res = [];

        if (col.schemaName)
            res.push(col.schemaName);
        if (col.typeName)
            res.push(col.typeName);

        res.push(col.fieldName);

        return res.join(".");
    };

    $scope.resultMode = function (paragraph, type) {
        return (paragraph.result === type);
    };

    $scope.getter = function (value) {
        return value;
    };

    $scope.showBarChart = function (paragraph) {
        paragraph.result = 'bar';

        var testdata = [
            {
                "key": "Quantity",
                "bar": true,
                "values": [[1136005200000, 1271000.0], [1138683600000, 1271000.0], [1141102800000, 1271000.0], [1143781200000, 0], [1146369600000, 0], [1149048000000, 0], [1151640000000, 0], [1154318400000, 0], [1156996800000, 0], [1159588800000, 3899486.0], [1162270800000, 3899486.0], [1164862800000, 3899486.0], [1167541200000, 3564700.0], [1170219600000, 3564700.0], [1172638800000, 3564700.0], [1175313600000, 2648493.0], [1177905600000, 2648493.0], [1180584000000, 2648493.0], [1183176000000, 2522993.0], [1185854400000, 2522993.0], [1188532800000, 2522993.0], [1191124800000, 2906501.0], [1193803200000, 2906501.0], [1196398800000, 2906501.0], [1199077200000, 2206761.0], [1201755600000, 2206761.0], [1204261200000, 2206761.0], [1206936000000, 2287726.0], [1209528000000, 2287726.0], [1212206400000, 2287726.0], [1214798400000, 2732646.0], [1217476800000, 2732646.0], [1220155200000, 2732646.0], [1222747200000, 2599196.0], [1225425600000, 2599196.0], [1228021200000, 2599196.0], [1230699600000, 1924387.0], [1233378000000, 1924387.0], [1235797200000, 1924387.0], [1238472000000, 1756311.0], [1241064000000, 1756311.0], [1243742400000, 1756311.0], [1246334400000, 1743470.0], [1249012800000, 1743470.0], [1251691200000, 1743470.0], [1254283200000, 1519010.0], [1256961600000, 1519010.0], [1259557200000, 1519010.0], [1262235600000, 1591444.0], [1264914000000, 1591444.0], [1267333200000, 1591444.0], [1270008000000, 1543784.0], [1272600000000, 1543784.0], [1275278400000, 1543784.0], [1277870400000, 1309915.0], [1280548800000, 1309915.0], [1283227200000, 1309915.0], [1285819200000, 1331875.0], [1288497600000, 1331875.0], [1291093200000, 1331875.0], [1293771600000, 1331875.0], [1296450000000, 1154695.0], [1298869200000, 1154695.0], [1301544000000, 1194025.0], [1304136000000, 1194025.0], [1306814400000, 1194025.0], [1309406400000, 1194025.0], [1312084800000, 1194025.0], [1314763200000, 1244525.0], [1317355200000, 475000.0], [1320033600000, 475000.0], [1322629200000, 475000.0], [1325307600000, 690033.0], [1327986000000, 690033.0], [1330491600000, 690033.0], [1333166400000, 514733.0], [1335758400000, 514733.0]]
            },
            {
                "key": "Price",
                "values": [[1136005200000, 71.89], [1138683600000, 75.51], [1141102800000, 68.49], [1143781200000, 62.72], [1146369600000, 70.39], [1149048000000, 59.77], [1151640000000, 57.27], [1154318400000, 67.96], [1156996800000, 67.85], [1159588800000, 76.98], [1162270800000, 81.08], [1164862800000, 91.66], [1167541200000, 84.84], [1170219600000, 85.73], [1172638800000, 84.61], [1175313600000, 92.91], [1177905600000, 99.8], [1180584000000, 121.191], [1183176000000, 122.04], [1185854400000, 131.76], [1188532800000, 138.48], [1191124800000, 153.47], [1193803200000, 189.95], [1196398800000, 182.22], [1199077200000, 198.08], [1201755600000, 135.36], [1204261200000, 125.02], [1206936000000, 143.5], [1209528000000, 173.95], [1212206400000, 188.75], [1214798400000, 167.44], [1217476800000, 158.95], [1220155200000, 169.53], [1222747200000, 113.66], [1225425600000, 107.59], [1228021200000, 92.67], [1230699600000, 85.35], [1233378000000, 90.13], [1235797200000, 89.31], [1238472000000, 105.12], [1241064000000, 125.83], [1243742400000, 135.81], [1246334400000, 142.43], [1249012800000, 163.39], [1251691200000, 168.21], [1254283200000, 185.35], [1256961600000, 188.5], [1259557200000, 199.91], [1262235600000, 210.732], [1264914000000, 192.063], [1267333200000, 204.62], [1270008000000, 235.0], [1272600000000, 261.09], [1275278400000, 256.88], [1277870400000, 251.53], [1280548800000, 257.25], [1283227200000, 243.1], [1285819200000, 283.75], [1288497600000, 300.98], [1291093200000, 311.15], [1293771600000, 322.56], [1296450000000, 339.32], [1298869200000, 353.21], [1301544000000, 348.5075], [1304136000000, 350.13], [1306814400000, 347.83], [1309406400000, 335.67], [1312084800000, 390.48], [1314763200000, 384.83], [1317355200000, 381.32], [1320033600000, 404.78], [1322629200000, 382.2], [1325307600000, 405.0], [1327986000000, 456.48], [1330491600000, 542.44], [1333166400000, 599.55], [1335758400000, 583.98]]
            }
        ].map(function (series) {
                series.values = series.values.map(function (d) {
                    return {x: d[0], y: d[1]}
                });
                return series;
            });

        var chart;

        nv.addGraph(function () {
            chart = nv.models.linePlusBarChart()
                .margin({top: 50, right: 60, bottom: 30, left: 70})
                .legendRightAxisHint(' [Using Right Axis]')
                .color(d3.scale.category10().range());
            chart.xAxis.tickFormat(function (d) {
                return d3.time.format('%x')(new Date(d))
            }).showMaxMin(false);

            chart.y1Axis.tickFormat(function (d) {
                return '$' + d3.format(',f')(d)
            });

            chart.bars.forceY([0]).padData(false);

            chart.x2Axis.tickFormat(function (d) {
                return d3.time.format('%x')(new Date(d))
            }).showMaxMin(false);

            var z = d3.select('#chart svg');

            z.datum(testdata)
                .transition().duration(500).call(chart);

            nv.utils.windowResize(chart.update);

            chart.dispatch.on('stateChange', function (e) {
                nv.log('New State:', JSON.stringify(e));
            });

            return chart;
        });
    }
}]);
