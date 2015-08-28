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

// Controller for Caches screen.
controlCenterModule.controller('cachesController', [
        '$scope', '$controller', '$http', '$timeout', '$common', '$focus', '$confirm', '$copy', '$table', '$preview',
        function ($scope, $controller, $http, $timeout, $common, $focus, $confirm, $copy, $table, $preview) {
            // Initialize the super class and extend it.
            angular.extend(this, $controller('save-remove', {$scope: $scope}));

            $scope.joinTip = $common.joinTip;
            $scope.getModel = $common.getModel;
            $scope.javaBuildInClasses = $common.javaBuildInClasses;
            $scope.compactJavaName = $common.compactJavaName;

            $scope.tableReset = $table.tableReset;
            $scope.tableNewItem = $table.tableNewItem;
            $scope.tableNewItemActive = $table.tableNewItemActive;
            $scope.tableEditing = $table.tableEditing;
            $scope.tableStartEdit = $table.tableStartEdit;
            $scope.tableRemove = function (item, field, index) {
                $table.tableRemove(item, field, index);

                $common.markChanged($scope.ui.inputForm, 'cacheBackupItemChanged');
            };

            $scope.tableSimpleSave = $table.tableSimpleSave;
            $scope.tableSimpleSaveVisible = $table.tableSimpleSaveVisible;
            $scope.tableSimpleUp = $table.tableSimpleUp;
            $scope.tableSimpleDown = $table.tableSimpleDown;
            $scope.tableSimpleDownVisible = $table.tableSimpleDownVisible;

            $scope.tablePairSave = $table.tablePairSave;
            $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

            $scope.previewInit = $preview.previewInit;

            $scope.formChanged = $common.formChanged;

            $scope.hidePopover = $common.hidePopover;

            var showPopoverMessage = $common.showPopoverMessage;

            $scope.atomicities = $common.mkOptions(['ATOMIC', 'TRANSACTIONAL']);

            $scope.cacheModes = $common.mkOptions(['PARTITIONED', 'REPLICATED', 'LOCAL']);

            $scope.atomicWriteOrderModes = $common.mkOptions(['CLOCK', 'PRIMARY']);

            $scope.memoryModes = $common.mkOptions(['ONHEAP_TIERED', 'OFFHEAP_TIERED', 'OFFHEAP_VALUES']);

            $scope.evictionPolicies = [
                {value: 'LRU', label: 'LRU'},
                {value: 'RND', label: 'Random'},
                {value: 'FIFO', label: 'FIFO'},
                {value: 'SORTED', label: 'Sorted'},
                {value: undefined, label: 'Not set'}
            ];

            $scope.rebalanceModes = $common.mkOptions(['SYNC', 'ASYNC', 'NONE']);

            $scope.cacheStoreFactories = [
                {value: 'CacheJdbcPojoStoreFactory', label: 'JDBC POJO store factory'},
                {value: 'CacheJdbcBlobStoreFactory', label: 'JDBC BLOB store factory'},
                {value: 'CacheHibernateBlobStoreFactory', label: 'Hibernate BLOB store factory'},
                {value: undefined, label: 'Not set'}
            ];

            $scope.cacheStoreJdbcDialects = [
                {value: 'Oracle', label: 'Oracle'},
                {value: 'DB2', label: 'IBM DB2'},
                {value: 'SQLServer', label: 'Microsoft SQL Server'},
                {value: 'MySQL', label: 'My SQL'},
                {value: 'PostgreSQL', label: 'Postgre SQL'},
                {value: 'H2', label: 'H2 database'}
            ];

            $scope.ui = {expanded: false};

            $scope.toggleExpanded = function () {
                $scope.ui.expanded = !$scope.ui.expanded;

                $common.hidePopover();
            };

            $scope.panels = {activePanels: [0]};

            $scope.$watchCollection('panels.activePanels', function () {
                $timeout(function() {
                    $common.previewHeightUpdate();
                })
            });

            $scope.general = [];
            $scope.advanced = [];

            $http.get('/models/caches.json')
                .success(function (data) {
                    $scope.screenTip = data.screenTip;
                    $scope.general = data.general;
                    $scope.advanced = data.advanced;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            $scope.caches = [];
            $scope.queryMetadata = [];
            $scope.storeMetadata = [];

            $scope.preview = {
                general: {xml: '', java: '', allDefaults: true},
                memory: {xml: '', java: '', allDefaults: true},
                query: {xml: '', java: '', allDefaults: true},
                store: {xml: '', java: '', allDefaults: true},
                concurrency: {xml: '', java: '', allDefaults: true},
                rebalance: {xml: '', java: '', allDefaults: true},
                serverNearCache: {xml: '', java: '', allDefaults: true},
                statistics: {xml: '', java: '', allDefaults: true}
            };

            $scope.required = function (field) {
                var model = $common.isDefined(field.path) ? field.path + '.' + field.model : field.model;

                var backupItem = $scope.backupItem;

                var memoryMode = backupItem.memoryMode;

                var onHeapTired = memoryMode == 'ONHEAP_TIERED';
                var offHeapTired = memoryMode == 'OFFHEAP_TIERED';

                var offHeapMaxMemory = backupItem.offHeapMaxMemory;

                if (model == 'offHeapMaxMemory' && offHeapTired)
                    return true;

                if (model == 'evictionPolicy.kind' && onHeapTired)
                    return backupItem.swapEnabled || ($common.isDefined(offHeapMaxMemory) && offHeapMaxMemory >= 0);

                return false;
            };

            $scope.tableSimpleValid = function (item, field, fx, index) {
                if (!$common.isValidJavaClass('SQL function', fx, false, $table.tableFieldId(index, 'SqlFx')))
                    return $table.tableFocusInvalidField(index, 'SqlFx');

                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.indexOf(model, fx);

                    // Found duplicate.
                    if (idx >= 0 && idx != index)
                        return $common.showPopoverMessage(null, null, $table.tableFieldId(index, 'SqlFx'), 'SQL function with such class name already exists!');
                }

                return true;
            };

            $scope.tablePairValid = function (item, field, index) {
                var pairValue = $table.tablePairValue(field, index);

                if (!$common.isValidJavaClass('Indexed type key', pairValue.key, true, $table.tableFieldId(index, 'KeyIndexedType')))
                    return $table.tableFocusInvalidField(index, 'KeyIndexedType');

                if (!$common.isValidJavaClass('Indexed type value', pairValue.value, true, $table.tableFieldId(index, 'ValueIndexedType')))
                    return $table.tableFocusInvalidField(index, 'ValueIndexedType');

                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.findIndex(model, function (pair) {
                        return pair.keyClass == pairValue.key
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index)
                        return $common.showPopoverMessage(null, null, $table.tableFieldId(index, 'KeyIndexedType'), 'Indexed type with such key class already exists!');
                }

                return true;
            };

            // When landing on the page, get caches and show them.
            $http.post('caches/list')
                .success(function (data) {
                    $scope.spaces = data.spaces;
                    $scope.caches = data.caches;
                    $scope.clusters = data.clusters;

                    var metadatas = _.map(data.metadatas, function (meta) {
                        return {value: meta._id, label: meta.name, kind: meta.kind, meta: meta}
                    });

                    _.forEach(metadatas, function (meta) {
                        var kind = meta.kind;

                        if (kind == 'query' || kind == 'both')
                            $scope.queryMetadata.push(meta);

                        if (kind == 'store' || kind == 'both')
                            $scope.storeMetadata.push(meta);
                    });

                    var restoredItem = angular.fromJson(sessionStorage.cacheBackupItem);

                    if (restoredItem) {
                        restoredItem.queryMetadata = _.filter(restoredItem.queryMetadata, function (metaId) {
                            return _.findIndex($scope.metadatas, function (scopeMeta) {
                                    return scopeMeta.value == metaId;
                                }) >= 0;
                        });

                        restoredItem.storeMetadata = _.filter(restoredItem.storeMetadata, function (metaId) {
                            return _.findIndex($scope.metadatas, function (scopeMeta) {
                                    return scopeMeta.value == metaId;
                                }) >= 0;
                        });

                        if (restoredItem._id) {
                            var idx = _.findIndex($scope.caches, function (cache) {
                                return cache._id == restoredItem._id;
                            });

                            if (idx >= 0) {
                                var cache = $scope.caches[idx];

                                var restoredSelectedItem = angular.fromJson(sessionStorage.cacheSelectedItem);

                                // Clusters not changed by user. We should take clusters from server as they could be changed on Clusters screen.
                                if (restoredSelectedItem && _.isEqual(restoredItem.clusters, restoredSelectedItem.clusters)) {
                                    restoredItem.clusters = [];

                                    _.forEach(cache.clusters, function (cache) {
                                        restoredItem.clusters.push(cache)
                                    });
                                }
                                else {
                                    // Clusters changed by user. We need to remove deleted clusters (if any).
                                    restoredItem.clusters = _.filter(restoredItem.clusters, function (clusterId) {
                                        return _.findIndex($scope.clusters, function (scopeCluster) {
                                                return scopeCluster.value == clusterId;
                                            }) >= 0;
                                    });
                                }

                                $scope.selectItem(cache, restoredItem, sessionStorage.cacheBackupItemChanged);
                            }
                            else
                                sessionStorage.removeItem('cacheBackupItem');
                        }
                        else
                            $scope.selectItem(undefined, restoredItem, sessionStorage.cacheBackupItemChanged)
                    }
                    else if ($scope.caches.length > 0)
                        $scope.selectItem($scope.caches[0]);

                    $scope.$watch('backupItem', function (val) {
                        if (val) {
                            sessionStorage.cacheBackupItem = angular.toJson(val);

                            var qryMeta = _.reduce($scope.queryMetadata, function(memo, meta){
                                if (_.contains(val.queryMetadata, meta.value)) {
                                    memo.push(meta.meta);
                                }

                                return memo;
                            }, []);

                            var storeMeta = _.reduce($scope.storeMetadata, function(memo, meta){
                                if (_.contains(val.storeMetadata, meta.value)) {
                                    memo.push(meta.meta);
                                }

                                return memo;
                            }, []);

                            var varName = 'cache';

                            $scope.preview.general.xml = $generatorXml.cacheGeneral(val).join('');
                            $scope.preview.general.java = $generatorJava.cacheGeneral(val, varName).join('');
                            $scope.preview.general.allDefaults = $common.isEmptyString($scope.preview.general.xml);

                            $scope.preview.memory.xml = $generatorXml.cacheMemory(val).join('');
                            $scope.preview.memory.java = $generatorJava.cacheMemory(val, varName).join('');
                            $scope.preview.memory.allDefaults = $common.isEmptyString($scope.preview.memory.xml);

                            $scope.preview.query.xml = $generatorXml.cacheMetadatas(qryMeta, null, $generatorXml.cacheQuery(val)).join('');
                            $scope.preview.query.java = $generatorJava.cacheMetadatas(qryMeta, null, varName, $generatorJava.cacheQuery(val, varName)).join('');
                            $scope.preview.query.allDefaults = $common.isEmptyString($scope.preview.query.xml);

                            $scope.preview.store.xml = $generatorXml.cacheMetadatas(null, storeMeta, $generatorXml.cacheStore(val)).join('');
                            $scope.preview.store.java = $generatorJava.cacheMetadatas(null, storeMeta, varName, $generatorJava.cacheStore(val, varName)).join('');
                            $scope.preview.store.allDefaults = $common.isEmptyString($scope.preview.store.xml);

                            $scope.preview.concurrency.xml = $generatorXml.cacheConcurrency(val).join('');
                            $scope.preview.concurrency.java = $generatorJava.cacheConcurrency(val, varName).join('');
                            $scope.preview.concurrency.allDefaults = $common.isEmptyString($scope.preview.concurrency.xml);

                            $scope.preview.rebalance.xml = $generatorXml.cacheRebalance(val).join('');
                            $scope.preview.rebalance.java = $generatorJava.cacheRebalance(val, varName).join('');
                            $scope.preview.rebalance.allDefaults = $common.isEmptyString($scope.preview.rebalance.xml);

                            $scope.preview.serverNearCache.xml = $generatorXml.cacheServerNearCache(val).join('');
                            $scope.preview.serverNearCache.java = $generatorJava.cacheServerNearCache(val, varName).join('');
                            $scope.preview.serverNearCache.allDefaults = $common.isEmptyString($scope.preview.serverNearCache.xml);

                            $scope.preview.statistics.xml = $generatorXml.cacheStatistics(val).join('');
                            $scope.preview.statistics.java = $generatorJava.cacheStatistics(val, varName).join('');
                            $scope.preview.statistics.allDefaults = $common.isEmptyString($scope.preview.statistics.xml);

                            $common.markChanged($scope.ui.inputForm, 'cacheBackupItemChanged');
                        }
                    }, true);

                    $timeout(function () {
                        $common.initPreview();
                    })
               })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            $scope.selectItem = function (item, backup, changed) {
                function selectItem() {
                    $table.tableReset();

                    if (backup)
                        $scope.backupItem = backup;
                    else if (item)
                        $scope.backupItem = angular.copy(item);
                    else
                        $scope.backupItem = undefined;

                    $scope.selectedItem = item;

                    if (item)
                        sessionStorage.cacheSelectedItem = angular.toJson(item);
                    else
                        sessionStorage.removeItem('cacheSelectedItem');

                    $timeout(function () {
                        $common.previewHeightUpdate();
                    });

                    $timeout(function () {
                        if (changed)
                            $common.markChanged($scope.ui.inputForm, 'cacheBackupItemChanged');
                        else
                            $common.markPristine($scope.ui.inputForm, 'cacheBackupItemChanged');
                    }, 50);
                }

                $common.confirmUnsavedChanges($confirm, $scope.ui.inputForm, selectItem);

                $scope.ui.formTitle = $common.isDefined($scope.backupItem) && $scope.backupItem._id ?
                    'Selected cache: ' + $scope.backupItem.name : 'New cache';
            };

            // Add new cache.
            $scope.createItem = function () {
                $table.tableReset();

                $timeout(function () {
                    $common.ensureActivePanel($scope.panels, 'general', 'cacheName');
                });

                var newItem = {
                    space: $scope.spaces[0]._id,
                    cacheModes: 'PARTITIONED',
                    atomicityMode: 'ATOMIC',
                    readFromBackup: true,
                    copyOnRead: true,
                    clusters: [],
                    queryMetadata: [],
                    spaceMetadata: []
                };

                $scope.selectItem(undefined, newItem);
            };

            // Check cache logical consistency.
            function validate(item) {
                if ($common.isEmptyString(item.name))
                    return showPopoverMessage($scope.panels, 'general', 'cacheName', 'Name should not be empty');
                        sessionStorage.removeItem('cacheSelectedItem');

                if (item.memoryMode == 'OFFHEAP_TIERED' && item.offHeapMaxMemory == null)
                    return showPopoverMessage($scope.panels, 'memory', 'offHeapMaxMemory',
                        'Off-heap max memory should be specified');

                var cacheStoreFactorySelected = item.cacheStoreFactory && item.cacheStoreFactory.kind;

                if (cacheStoreFactorySelected) {
                    if (item.cacheStoreFactory.kind == 'CacheJdbcPojoStoreFactory') {
                        if ($common.isEmptyString(item.cacheStoreFactory.CacheJdbcPojoStoreFactory.dataSourceBean))
                            return showPopoverMessage($scope.panels, 'store', 'dataSourceBean',
                                'Data source bean should not be empty');

                        if (!item.cacheStoreFactory.CacheJdbcPojoStoreFactory.dialect)
                            return showPopoverMessage($scope.panels, 'store', 'dialect',
                                'Dialect should not be empty');
                    }

                    if (item.cacheStoreFactory.kind == 'CacheJdbcBlobStoreFactory') {
                        if ($common.isEmptyString(item.cacheStoreFactory.CacheJdbcBlobStoreFactory.user))
                            return showPopoverMessage($scope.panels, 'store', 'user',
                                'User should not be empty');

                        if ($common.isEmptyString(item.cacheStoreFactory.CacheJdbcBlobStoreFactory.dataSourceBean))
                            return showPopoverMessage($scope.panels, 'store', 'dataSourceBean',
                                'Data source bean should not be empty');
                    }
                }

                if ((item.readThrough || item.writeThrough) && !cacheStoreFactorySelected)
                    return showPopoverMessage($scope.panels, 'store', 'cacheStoreFactory',
                        (item.readThrough ? 'Read' : 'Write') + ' through are enabled but store is not configured!');

                if (item.writeBehindEnabled && !cacheStoreFactorySelected)
                    return showPopoverMessage($scope.panels, 'store', 'cacheStoreFactory',
                        'Write behind enabled but store is not configured!');

                if (cacheStoreFactorySelected && !(item.readThrough || item.writeThrough))
                    return showPopoverMessage($scope.panels, 'store', 'readThrough',
                        'Store is configured but read/write through are not enabled!');

                return true;
            }

            // Save cache into database.
            function save(item) {
                $http.post('caches/save', item)
                    .success(function (_id) {
                        $common.markPristine($scope.ui.inputForm, 'cacheBackupItemChanged');

                        var idx = _.findIndex($scope.caches, function (cache) {
                            return cache._id == _id;
                        });

                        if (idx >= 0)
                            angular.extend($scope.caches[idx], item);
                        else {
                            item._id = _id;

                            $scope.caches.push(item);
                        }

                        $scope.selectItem(item);

                        $common.showInfo('Cache "' + item.name + '" saved.');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            }

            // Save cache.
            $scope.saveItem = function () {
                $table.tableReset();

                var item = $scope.backupItem;

                if (validate(item))
                    save(item);
            };

            // Save cache with new name.
            $scope.copyItem = function () {
                $table.tableReset();

                if (validate($scope.backupItem))
                    $copy.show($scope.backupItem.name).then(function (newName) {
                        var item = angular.copy($scope.backupItem);

                        item._id = undefined;
                        item.name = newName;

                        save(item);
                    });
            };

            // Remove cache from db.
            $scope.removeItem = function () {
                $table.tableReset();

                var selectedItem = $scope.selectedItem;

                $confirm.show('Are you sure you want to remove cache: "' + selectedItem.name + '"?').then(
                    function () {
                        $common.markPristine($scope.ui.inputForm, 'cacheBackupItemChanged');

                        var _id = selectedItem._id;

                        $http.post('caches/remove', {_id: _id})
                            .success(function () {
                                $common.showInfo('Cache has been removed: ' + selectedItem.name);

                                var caches = $scope.caches;

                                var idx = _.findIndex(caches, function (cache) {
                                    return cache._id == _id;
                                });

                                if (idx >= 0) {
                                    caches.splice(idx, 1);

                                    if (caches.length > 0)
                                        $scope.selectItem(caches[0]);
                                    else
                                        $scope.selectItem(undefined, undefined);
                                }
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                    }
                );
            };

            // Remove all caches from db.
            $scope.removeAllItems = function () {
                $table.tableReset();

                $confirm.show('Are you sure you want to remove all caches?').then(
                    function () {
                        $common.markPristine($scope.ui.inputForm, 'cacheBackupItemChanged');

                        $http.post('caches/remove/all')
                            .success(function () {
                                $common.showInfo('All caches have been removed');

                                $scope.caches = [];

                                $scope.selectItem(undefined, undefined);
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                    }
                );
            };
        }]
);
