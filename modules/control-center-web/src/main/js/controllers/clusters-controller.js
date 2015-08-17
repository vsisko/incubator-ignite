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

controlCenterModule.controller('clustersController', ['$scope', '$http', '$common', '$focus', '$confirm', '$copy', '$table',
    function ($scope, $http, $common, $focus, $confirm, $copy, $table) {
        $scope.joinTip = $common.joinTip;
        $scope.getModel = $common.getModel;

        $scope.tableReset = $table.tableReset;
        $scope.tableNewItem = $table.tableNewItem;
        $scope.tableNewItemActive = $table.tableNewItemActive;
        $scope.tableEditing = $table.tableEditing;
        $scope.tableStartEdit = $table.tableStartEdit;
        $scope.tableRemove = $table.tableRemove;

        $scope.tableSimpleSave = $table.tableSimpleSave;
        $scope.tableSimpleSaveVisible = $table.tableSimpleSaveVisible;
        $scope.tableSimpleUp = $table.tableSimpleUp;
        $scope.tableSimpleDown = $table.tableSimpleDown;
        $scope.tableSimpleDownVisible = $table.tableSimpleDownVisible;

        $scope.hidePopover = $common.hidePopover;

        $scope.templates = [
            {value: {discovery: {kind: 'Multicast', Vm: {addresses: ['127.0.0.1:47500..47510']}, Multicast: {}}},label: 'multicast'},
            {value: {discovery: {kind: 'Vm', Vm: {addresses: ['127.0.0.1:47500..47510']}}}, label: 'local'}
        ];

        $scope.template = $scope.templates[0].value;

        $scope.discoveries = [
            {value: 'Vm', label: 'static IPs'},
            {value: 'Multicast', label: 'multicast'},
            {value: 'S3', label: 'AWS S3'},
            {value: 'Cloud', label: 'apache jclouds'},
            {value: 'GoogleStorage', label: 'google cloud storage'},
            {value: 'Jdbc', label: 'JDBC'},
            {value: 'SharedFs', label: 'shared filesystem'}
        ];

        $scope.swapSpaceSpis = [
            {value: 'FileSwapSpaceSpi', label: 'File-based swap'},
            {value: undefined, label: 'Not set'}
        ];

        $scope.events = [];

        for (var eventGroupName in eventGroups) {
            if (eventGroups.hasOwnProperty(eventGroupName)) {
                $scope.events.push({value: eventGroupName, label: eventGroupName});
            }
        }

        $scope.cacheModes = [
            {value: 'LOCAL', label: 'LOCAL'},
            {value: 'REPLICATED', label: 'REPLICATED'},
            {value: 'PARTITIONED', label: 'PARTITIONED'}
        ];

        $scope.deploymentModes = [
            {value: 'PRIVATE', label: 'PRIVATE'},
            {value: 'ISOLATED', label: 'ISOLATED'},
            {value: 'SHARED', label: 'SHARED'},
            {value: 'CONTINUOUS', label: 'CONTINUOUS'}
        ];

        $scope.transactionConcurrency = [
            {value: 'OPTIMISTIC', label: 'OPTIMISTIC'},
            {value: 'PESSIMISTIC', label: 'PESSIMISTIC'}
        ];

        $scope.transactionIsolation = [
            {value: 'READ_COMMITTED', label: 'READ_COMMITTED'},
            {value: 'REPEATABLE_READ', label: 'REPEATABLE_READ'},
            {value: 'SERIALIZABLE', label: 'SERIALIZABLE'}
        ];

        $scope.segmentationPolicy = [
            {value: 'RESTART_JVM', label: 'RESTART_JVM'},
            {value: 'STOP', label: 'STOP'},
            {value: 'NOOP', label: 'NOOP'}
        ];

        $scope.marshallers = [
            {value: 'OptimizedMarshaller', label: 'OptimizedMarshaller'},
            {value: 'JdkMarshaller', label: 'JdkMarshaller'}
        ];

        $scope.ui = {expanded: false};

        $scope.toggleExpanded = function () {
            $scope.ui.expanded = !$scope.ui.expanded;

            $common.hidePopover();
        };

        $scope.panels = {activePanels: [0]};

        var simpleTables = {
            addresses: {msg: 'Such IP address already exists!', id: 'IpAddress'},
            regions: {msg: 'Such region already exists!', id: 'Region'},
            zones: {msg: 'Such zone already exists!', id: 'Zone'}
        };

        function focusInvalidField(index, id) {
            $focus(index < 0 ? 'new' + id : 'cur' + id);

            return false;
        }

        $scope.tableSimpleValid = function (item, field, val, index) {
            var model = $common.getModel(item, field)[field.model];

            if ($common.isDefined(model)) {
                var idx = _.indexOf(model, val);

                // Found duplicate.
                if (idx >= 0 && idx != index) {
                    var simpleTable = simpleTables[field.model];

                    if (simpleTable) {
                        $common.showError(simpleTable.msg);

                        return focusInvalidField(index, simpleTable.id);
                    }
                }
            }

            return true;
        };

        $scope.clusters = [];

        $http.get('/models/clusters.json')
            .success(function (data) {
                $scope.screenTip = data.screenTip;
                $scope.templateTip = data.templateTip;

                $scope.general = data.general;
                $scope.advanced = data.advanced;
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });

        // When landing on the page, get clusters and show them.
        $http.post('clusters/list')
            .success(function (data) {
                $scope.caches = data.caches;
                $scope.spaces = data.spaces;
                $scope.clusters = data.clusters;

                var restoredItem = angular.fromJson(sessionStorage.clusterBackupItem);

                if (restoredItem) {
                    if (restoredItem._id) {
                        var idx = _.findIndex($scope.clusters, function (cluster) {
                            return cluster._id == restoredItem._id;
                        });

                        if (idx >= 0) {
                            // Remove deleted caches.
                            restoredItem.caches = _.filter(restoredItem.caches, function (cacheId) {
                                return _.findIndex($scope.caches, function (scopeCache) {
                                        return scopeCache.value == cacheId;
                                    }) >= 0;
                            });

                            $scope.selectedItem = $scope.clusters[idx];
                            $scope.backupItem = restoredItem;
                        }
                        else
                            sessionStorage.removeItem('clusterBackupItem');
                    }
                    else
                        $scope.backupItem = restoredItem;
                }
                else if ($scope.clusters.length > 0)
                    $scope.selectItem($scope.clusters[0]);

                $scope.$watch('backupItem', function (val) {
                    if (val)
                        sessionStorage.clusterBackupItem = angular.toJson(val);
                }, true);
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });

        $scope.selectItem = function (item) {
            $table.tableReset();

            $scope.selectedItem = item;
            $scope.backupItem = angular.copy(item);
        };

        // Add new cluster.
        $scope.createItem = function () {
            $table.tableReset();
            $common.ensureActivePanel($scope.panels, "general-data");

            $scope.selectedItem = undefined;

            $scope.backupItem = angular.copy($scope.template);
            $scope.backupItem.caches = [];
            $scope.backupItem.space = $scope.spaces[0]._id;
        };

        $scope.indexOfCache = function (cacheId) {
            return _.findIndex($scope.caches, function (cache) {
                return cache.value == cacheId;
            });
        };

        // Check cluster logical consistency.
        function validate(item) {
            if ($common.isEmptyString(item.name))
                return $common.showPopoverMessage($scope.panels, 'general-data', 'clusterName', 'Name should not be empty');

            if (item.discovery.kind == 'Vm' && item.discovery.Vm.addresses.length == 0)
                return $common.showPopoverMessage($scope.panels, 'general-data', 'addresses', 'Addresses are not specified');

            if (item.discovery.kind == 'S3' && $common.isEmptyString(item.discovery.S3.bucketName))
                return $common.showPopoverMessage($scope.panels, 'general-data', 'bucketName', 'Bucket name should not be empty');

            if (item.discovery.kind == 'Cloud') {
                if ($common.isEmptyString(item.discovery.Cloud.identity))
                    return $common.showPopoverMessage($scope.panels, 'general-data', 'identity', 'Identity should not be empty');

                if ($common.isEmptyString(item.discovery.Cloud.provider))
                    return $common.showPopoverMessage($scope.panels, 'general-data', 'provider', 'Provider should not be empty');
            }

            if (item.discovery.kind == 'GoogleStorage') {
                if ($common.isEmptyString(item.discovery.GoogleStorage.projectName))
                    return $common.showPopoverMessage($scope.panels, 'general-data', 'projectName', 'Project name should not be empty');

                if ($common.isEmptyString(item.discovery.GoogleStorage.bucketName))
                    return $common.showPopoverMessage($scope.panels, 'general-data', 'bucketName', 'Bucket name should not be empty');

                if ($common.isEmptyString(item.discovery.GoogleStorage.serviceAccountP12FilePath))
                    return $common.showPopoverMessage($scope.panels, 'general-data', 'serviceAccountP12FilePath', 'Private key path should not be empty');

                if ($common.isEmptyString(item.discovery.GoogleStorage.serviceAccountId))
                    return $common.showPopoverMessage($scope.panels, 'general-data', 'serviceAccountId', 'Account ID should not be empty');
            }

            if (!item.swapSpaceSpi || !item.swapSpaceSpi.kind && item.caches) {
                for (var i = 0; i < item.caches.length; i++) {
                    var idx = $scope.indexOfCache(item.caches[i]);

                    if (idx >= 0) {
                        var cache = $scope.caches[idx];

                        if (cache.swapEnabled) {
                            $scope.ui.expanded = true;

                            return $common.showPopoverMessage($scope.panels, 'swap-data', 'swapSpaceSpi',
                                'Swap space SPI is not configured, but cache "' + cache.label + '" configured to use swap!');
                        }
                    }
                }
            }

            return true;
        }

        // Save cluster in database.
        function save(item) {
            $http.post('clusters/save', item)
                .success(function (_id) {
                    var idx = _.findIndex($scope.clusters, function (cluster) {
                        return cluster._id == _id;
                    });

                    if (idx >= 0)
                        angular.extend($scope.clusters[idx], item);
                    else {
                        item._id = _id;

                        $scope.clusters.push(item);
                    }

                    $scope.selectItem(item);

                    $common.showInfo('Cluster "' + item.name + '" saved.');
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });
        }

        // Save cluster.
        $scope.saveItem = function () {
            $table.tableReset();

            var item = $scope.backupItem;

            if (validate(item))
                save(item);
        };

        // Save cluster with new name.
        $scope.saveItemAs = function () {
            $table.tableReset();

            if (validate($scope.backupItem))
                $copy.show($scope.backupItem.name).then(function (newName) {
                    var item = angular.copy($scope.backupItem);

                    item._id = undefined;
                    item.name = newName;

                    save(item);
                });
        };

        // Remove cluster from db.
        $scope.removeItem = function () {
            $table.tableReset();

            var selectedItem = $scope.selectedItem;

            $confirm.show('Are you sure you want to remove cluster: "' + selectedItem.name + '"?').then(
                function () {
                    var _id = selectedItem._id;

                    $http.post('clusters/remove', {_id: _id})
                        .success(function () {
                            $common.showInfo('Cluster has been removed: ' + selectedItem.name);

                            var clusters = $scope.clusters;

                            var idx = _.findIndex(clusters, function (cluster) {
                                return cluster._id == _id;
                            });

                            if (idx >= 0) {
                                clusters.splice(idx, 1);

                                if (clusters.length > 0)
                                    $scope.selectItem(clusters[0]);
                                else {
                                    $scope.selectedItem = undefined;
                                    $scope.backupItem = undefined;
                                }
                            }
                        })
                        .error(function (errMsg) {
                            $common.showError(errMsg);
                        });
                }
            );
        };
    }]
);
