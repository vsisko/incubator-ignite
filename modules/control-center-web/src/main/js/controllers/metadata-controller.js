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

controlCenterModule.controller('metadataController', [
        '$scope', '$controller', '$http', '$modal', '$common', '$timeout', '$focus', '$confirm', '$copy', '$table',
        function ($scope, $controller, $http, $modal, $common, $timeout, $focus, $confirm, $copy, $table) {
            // Initialize the super class and extend it.
            angular.extend(this, $controller('agent-download', {$scope: $scope}));

            $scope.joinTip = $common.joinTip;
            $scope.getModel = $common.getModel;
            $scope.javaBuildInClasses = $common.javaBuildInClasses;

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

            $scope.tablePairStartEdit = $table.tablePairStartEdit;
            $scope.tablePairSave = $table.tablePairSave;
            $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

            $scope.compactJavaName = $common.compactJavaName;

            var presets = [
                {
                    db: 'oracle',
                    jdbcDriverClass: 'oracle.jdbc.OracleDriver',
                    jdbcUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
                    user: 'system'
                },
                {
                    db: 'db2',
                    jdbcDriverClass: 'com.ibm.db2.jcc.DB2Driver',
                    jdbcUrl: 'jdbc:db2://[host]:[port]/[database]',
                    user: 'db2admin'
                },
                {
                    db: 'mssql',
                    jdbcDriverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                    jdbcUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]',
                    user: 'sa'
                },
                {
                    db: 'postgre',
                    jdbcDriverClass: 'org.postgresql.Driver',
                    jdbcUrl: 'jdbc:postgresql://[host]:[port]/[database]',
                    user: 'sa'
                },
                {
                    db: 'mysql',
                    jdbcDriverClass: 'com.mysql.jdbc.Driver',
                    jdbcUrl: 'jdbc:mysql://[host]:[port]/[database]',
                    user: 'root'
                },
                {
                    db: 'h2',
                    jdbcDriverClass: 'org.h2.Driver',
                    jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
                    user: 'sa'
                }
            ];

            $scope.preset = {
                db: 'unknown',
                jdbcDriverClass: '',
                jdbcDriverJar: '',
                jdbcUrl: 'jdbc:[database]',
                user: 'sa'
            };

            var jdbcDrivers = [];

            function _findPreset(jdbcDriverJar) {
                var idx = _.findIndex(jdbcDrivers, function (jdbcDriver) {
                   return  jdbcDriver.jdbcDriverJar == jdbcDriverJar;
                });

                if (idx >= 0) {
                    var jdbcDriverClass = jdbcDrivers[idx].jdbcDriverClass;

                    idx = _.findIndex(presets, function (preset) {
                        return preset.jdbcDriverClass == jdbcDriverClass;
                    });

                    if (idx >= 0)
                        return presets[idx];
                }

                return {
                    db: 'unknown',
                    jdbcDriverClass: '',
                    jdbcDriverJar: '',
                    jdbcUrl: 'jdbc:[database]',
                    user: 'sa'
                }
            }

            $scope.$watch('preset.jdbcDriverJar', function (jdbcDriverJar) {
                if (jdbcDriverJar) {
                    var newPreset = _findPreset(jdbcDriverJar);

                    $scope.preset.db = newPreset.db;
                    $scope.preset.jdbcDriverClass = newPreset.jdbcDriverClass;
                    $scope.preset.jdbcUrl = newPreset.jdbcUrl;
                    $scope.preset.user = newPreset.user;
                }
            }, true);

            $scope.jdbcTypes = $common.mkOptions($common.JDBC_TYPES);

            $scope.javaTypes = $common.mkOptions($common.javaBuildInClasses);

            $scope.sortDirections = [
                {value: false, label: 'ASC'},
                {value: true, label: 'DESC'}
            ];

            $scope.panels = {activePanels: [0, 1]};

            $scope.metadatas = [];

            $scope.isJavaBuildInClass = function () {
                var item = $scope.backupItem;

                if (item && item.keyType)
                    return $common.isJavaBuildInClass(item.keyType);

                return false;
            };

            $http.get('/models/metadata.json')
                .success(function (data) {
                    $scope.screenTip = data.screenTip;
                    $scope.templateTip = data.templateTip;
                    $scope.metadata = data.metadata;
                    $scope.metadataDb = data.metadataDb;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            function selectFirstItem() {
                if ($scope.metadatas.length > 0)
                    $scope.selectItem($scope.metadatas[0]);
            }

            function setSelectedAndBackupItem(sel, bak) {
                $table.tableReset();

                $scope.selectedItem = sel;
                $scope.backupItem = bak;
            }

            $scope.loadMeta = {action: 'connect'};
            $scope.loadMeta.tables = [];

            $scope.loadMeta.selectAll = function () {
                var allSelected = $scope.loadMeta.allSelected;

                _.forEach($scope.loadMeta.tables, function (table) {
                    table.use = allSelected;
                });
            };

            $scope.loadMeta.select = function () {
                $scope.loadMeta.allSelected = _.every($scope.loadMeta.tables, 'use', true);
            };

            // Pre-fetch modal dialogs.
            var loadMetaModal = $modal({scope: $scope, templateUrl: 'metadata/metadata-load', show: false});

            // Show load metadata modal.
            $scope.showLoadMetadataModal = function () {
                $http.post('/agent/drivers')
                    .success(function (drivers) {
                        if (drivers && drivers.length > 0) {
                            $scope.jdbcDriverJars = _.map(drivers, function (driver) {
                                return {value: driver.jdbcDriverJar, label: driver.jdbcDriverJar};
                            });

                            jdbcDrivers = drivers;

                            $scope.preset.jdbcDriverJar = drivers[0].jdbcDriverJar;

                            loadMetaModal.$promise.then(function () {
                                $scope.loadMeta.action = 'connect';
                                $scope.loadMeta.tables = [];

                                loadMetaModal.show();

                                $focus('jdbcUrl');
                            });
                        }
                        else
                            $common.showError('JDBC drivers not found!');
                    })
                    .error(function (errMsg, status) {
                        if (status == 503)
                            $scope.showDownloadAgent();
                        else
                            $common.showError(errMsg);
                    });
            };

            $scope.loadMetadataFromDb = function () {
                $http.post('/agent/metadata', $scope.preset)
                    .success(function (tables) {
                        $scope.loadMeta.tables = _.map(tables, function (tbl) {
                            return {schemaName: tbl.schema, tableName: tbl.tbl};
                        });
                        $scope.loadMeta.action = 'tables';
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            };

            $scope.downloadAgent = function () {
                loadMetaModal.hide();

                var lnk = document.createElement('a');

                lnk.setAttribute('href', '/agent/agent.zip');
                lnk.style.display = 'none';

                document.body.appendChild(lnk);

                lnk.click();

                document.body.removeChild(lnk);
            };

            $scope.saveSelectedMetadata = function (preset) {
                loadMetaModal.hide();

                $common.showError("Saving selected metadata not ready yet!");
            };

            // When landing on the page, get metadatas and show them.
            $http.post('metadata/list')
                .success(function (data) {
                    $scope.spaces = data.spaces;
                    $scope.metadatas = data.metadatas;

                    var restoredItem = angular.fromJson(sessionStorage.metadataBackupItem);

                    if (restoredItem) {
                        if (restoredItem._id) {
                            var idx = _.findIndex($scope.metadatas, function (metadata) {
                                return metadata._id == restoredItem._id;
                            });

                            if (idx >= 0) {
                                // Remove deleted metadata.
                                restoredItem.queryMetadata = _.filter(restoredItem.queryMetadata, function (metaId) {
                                    return _.findIndex($scope.metadatas, function (scopeMeta) {
                                            return scopeMeta.value == metaId;
                                        }) >= 0;
                                });

                                // Remove deleted metadata.
                                restoredItem.storeMetadata = _.filter(restoredItem.storeMetadata, function (metaId) {
                                    return _.findIndex($scope.metadatas, function (scopeMeta) {
                                            return scopeMeta.value == metaId;
                                        }) >= 0;
                                });

                                setSelectedAndBackupItem($scope.metadatas[idx], restoredItem);
                            }
                            else {
                                sessionStorage.removeItem('metadataBackupItem');

                                selectFirstItem();
                            }
                        }
                        else
                            setSelectedAndBackupItem(undefined, restoredItem);
                    }
                    else
                        selectFirstItem();

                    $timeout(function () {
                        $scope.$apply();
                    });

                    $scope.$watch('backupItem', function (val) {
                        if (val)
                            sessionStorage.metadataBackupItem = angular.toJson(val);
                    }, true);
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            $scope.selectItem = function (item) {
                setSelectedAndBackupItem(item, angular.copy(item));
            };

            // Add new metadata.
            $scope.createItem = function () {
                $table.tableReset();
                $common.ensureActivePanel($scope.panels, 'metadata-data');

                $scope.selectedItem = undefined;

                $scope.backupItem = {space: $scope.spaces[0]._id};
            };

            // Check metadata logical consistency.
            function validate(item) {
                /*
                 if (!$common.isValidJavaClass('Key type', item.keyType, true)) {
                 $focus('keyType');

                 return false;
                 }

                 if (!$common.isValidJavaClass('Value type', item.valueType, false)) {
                 $focus('valueType');

                 return false;
                 }

                 if ($common.isEmptyArray(item.queryFields) && $common.isEmptyArray(item.ascendingFields) &&
                 $common.isEmptyArray(item.descendingFields) && $common.isEmptyArray(item.textFields) &&
                 $common.isEmptyArray(item.groups)) {
                 $common.showError('SQL fields are not specified!');

                 return false;
                 }

                 var groups = item.groups;
                 if (groups && groups.length > 0) {
                 for (var i = 0; i < groups.length; i++) {
                 var group = groups[i];
                 var fields = group.fields;

                 if ($common.isEmptyArray(fields)) {
                 $common.showError('Group "' + group.name + '" has no fields.');

                 return false;
                 }

                 if (fields.length == 1) {
                 $common.showError('Group "' + group.name + '" has only one field.<br/> Consider to use ascending or descending fields.');

                 return false;
                 }
                 }
                 }

                 if ($common.isEmptyArray(item.keyFields) && !$common.isJavaBuildInClass(item.keyType)) {
                 $common.showError('Key fields are not specified!');

                 return false;
                 }
                 if ($common.isEmptyArray(item.valueFields)) {
                 $common.showError('Value fields are not specified!');

                 return false;
                 }
                 */

                return true;
            }

            // Save cache type metadata into database.
            function save(item) {
                $http.post('metadata/save', item)
                    .success(function (_id) {
                        $common.showInfo('Metadata "' + item.name + '" saved.');

                        var idx = _.findIndex($scope.metadatas, function (metadata) {
                            return metadata._id == _id;
                        });

                        if (idx >= 0)
                            angular.extend($scope.metadatas[idx], item);
                        else {
                            item._id = _id;

                            $scope.metadatas.push(item);
                        }

                        $scope.selectItem(item);

                        $common.showInfo('Cache type metadata"' + item.name + '" saved.');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            }

            // Save cache type metadata.
            $scope.saveItem = function () {
                $table.tableReset();

                var item = $scope.backupItem;

                if (validate(item))
                    save(item);
            };

            // Save cache type metadata with new name.
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

            $scope.removeItem = function () {
                $table.tableReset();

                var selectedItem = $scope.selectedItem;

                $confirm.show('Are you sure you want to remove cache type metadata: "' + selectedItem.name + '"?').then(
                    function () {
                        var _id = selectedItem._id;

                        $http.post('metadata/remove', {_id: _id})
                            .success(function () {
                                $common.showInfo('Cache type metadata has been removed: ' + selectedItem.name);

                                var metadatas = $scope.metadatas;

                                var idx = _.findIndex(metadatas, function (metadata) {
                                    return metadata._id == _id;
                                });

                                if (idx >= 0) {
                                    metadatas.splice(idx, 1);

                                    if (metadatas.length > 0)
                                        $scope.selectItem(metadatas[0]);
                                    else {
                                        $scope.selectedItem = undefined;
                                        $scope.backupItem = undefined;
                                    }
                                }
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                    });
            };

            function focusInvalidField(index, id) {
                $focus(index < 0 ? 'new' + id : 'cur' + id);

                return false;
            }

            $scope.tableSimpleValid = function (item, field, name, index) {
                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.indexOf(model, name);

                    // Found duplicate.
                    if (idx >= 0 && idx != index) {
                        $common.showError('Field with such name already exists!');

                        return focusInvalidField(index, 'TextField');
                    }
                }

                return true;
            };

            var pairFields = {
                queryFields: {msg: 'Query field class', id: 'QryField'},
                ascendingFields: {msg: 'Ascending field class', id: 'AscField'},
                descendingFields: {msg: 'Descending field class', id: 'DescField'}
            };

            $scope.tablePairValid = function (item, field, index) {
                var pairField = pairFields[field.model];

                var pairValue = $table.tablePairValue(field, index);

                if (pairField) {
                    if (!$common.isValidJavaClass(pairField.msg, pairValue.value, true))
                        return focusInvalidField(index, 'Value' + pairField.id);

                    var model = item[field.model];

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (pair) {
                            return pair.name == pairValue.key
                        });

                        // Found duplicate.
                        if (idx >= 0 && idx != index) {
                            $common.showError('Field with such name already exists!');

                            return focusInvalidField(index, 'Key' + pairField.id);
                        }
                    }
                }

                return true;
            };

            function tableDbFieldValue(field, index) {
                return index < 0
                    ? {databaseName: field.newDatabaseName, databaseType: field.newDatabaseType, javaName: field.newJavaName, javaType: field.newJavaType}
                    : {databaseName: field.curDatabaseName, databaseType: field.curDatabaseType, javaName: field.curJavaName, javaType: field.curJavaType}
            }

            $scope.tableDbFieldSaveVisible = function (field, index) {
                var dbFieldValue = tableDbFieldValue(field, index);

                return !$common.isEmptyString(dbFieldValue.databaseName) && $common.isDefined(dbFieldValue.databaseType) &&
                    !$common.isEmptyString(dbFieldValue.javaName) && $common.isDefined(dbFieldValue.javaType);
            };

            var dbFieldTables = {
                keyFields: {msg: 'Key field', id: 'KeyField'},
                valueFields: {msg: 'Value field', id: 'ValueField'}
            };

            $scope.tableDbFieldSave = function (field, index) {
                var dbFieldTable = dbFieldTables[field.model];

                if (dbFieldTable) {
                    var dbFieldValue = tableDbFieldValue(field, index);

                    var item = $scope.backupItem;

                    var model = item[field.model];

                    if (!$common.isValidJavaIdentifier(dbFieldTable.msg + ' java name', dbFieldValue.javaName))
                        return focusInvalidField(index, 'JavaName' + dbFieldTable.id);

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (dbMeta) {
                            return dbMeta.databaseName == dbFieldValue.databaseName;
                        });

                        // Found duplicate.
                        if (idx >= 0 && index != idx) {
                            $common.showError('Field with such database name already exists!');

                            return focusInvalidField(index, 'DatabaseName' + dbFieldTable.id);
                        }

                        idx = _.findIndex(model, function (dbMeta) {
                            return dbMeta.javaName == dbFieldValue.javaName;
                        });

                        // Found duplicate.
                        if (idx >= 0 && index != idx) {
                            $common.showError('Field with such java name already exists!');

                            return focusInvalidField(index, 'JavaName' + dbFieldTable.id);
                        }

                        if (index < 0) {
                                model.push(dbFieldValue);
                        }
                        else {
                            var dbField = model[index];

                            dbField.databaseName = dbFieldValue.databaseName;
                            dbField.databaseType = dbFieldValue.databaseType;
                            dbField.javaName = dbFieldValue.javaName;
                            dbField.javaType = dbFieldValue.javaType;
                        }
                    }
                    else {
                        model = [dbFieldValue];

                        item[field.model] = model;
                    }

                    if (index < 0)
                        $table.tableNewItem(field);
                    else  if (index < model.length - 1)
                        $table.tableStartEdit(item, field, index + 1);
                    else
                        $table.tableNewItem(field);
                }
            };

            function tableGroupValue(field, index) {
                return index < 0 ? field.newGroupName : field.curGroupName;
            }

            $scope.tableGroupSaveVisible = function (field, index) {
                return !$common.isEmptyString(tableGroupValue(field, index));
            };

            $scope.tableGroupSave = function (field, index) {
                var groupName = tableGroupValue(field, index);

                var groups = $scope.backupItem.groups;

                if ($common.isDefined(groups)) {
                    var idx = _.findIndex(groups, function (group) {
                        return group.name == groupName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index) {
                        $common.showError('Group with such name already exists!');

                        return focusInvalidField(index, 'GroupName');
                    }
                }

                var item = $scope.backupItem;

                if (index < 0) {
                    var newGroup = {name: groupName};

                    if (item.groups)
                        item.groups.push(newGroup);
                    else
                        item.groups = [newGroup];
                }
                else
                    item.groups[index].name = groupName;

                if (index < 0)
                    $scope.tableGroupNewItem(field, item.groups.length - 1);
                else {
                    var group = item.groups[index];

                    if (group.fields || group.fields.length > 0)
                        $scope.tableGroupItemStartEdit(field, index, 0);
                    else
                        $scope.tableGroupNewItem(field, index);
                }
            };

            $scope.tableGroupNewItem = function (field, groupIndex) {
                var groupName = $scope.backupItem.groups[groupIndex].name;

                $table.tableNewItem({ui: 'table-query-group-fields', model: groupName});

                field.newFieldName = null;
                field.newClassName = null;
                field.newDirection = false;
            };

            $scope.tableGroupNewItemActive = function (groupIndex) {
                var groups = $scope.backupItem.groups;

                if (groups) {
                    var group = groups[groupIndex];

                    if (group) {
                        var groupName = group.name;

                        return $table.tableNewItemActive({model: groupName});
                    }
                }

                return false;
            };

            $scope.tableGroupItemEditing = function (groupIndex, index) {
                var groups = $scope.backupItem.groups;

                if (groups) {
                    var group = groups[groupIndex];

                    if (group)
                        return $table.tableEditing({model: group.name}, index);
                }

                return false;
            };

            function tableGroupItemValue(field, index) {
                return index < 0
                    ? {name: field.newFieldName, className: field.newClassName, direction: field.newDirection}
                    : {name: field.curFieldName, className: field.curClassName, direction: field.curDirection};
            }

            $scope.tableGroupItemStartEdit = function (field, groupIndex, index) {
                var groups = $scope.backupItem.groups;

                var group = groups[groupIndex];

                $table.tableState(group.name, index);

                var groupItem = group.fields[index];

                field.curFieldName = groupItem.name;
                field.curClassName = groupItem.className;
                field.curDirection = groupItem.direction;

                $focus('curFieldName');
            };

            $scope.tableGroupItemSaveVisible = function (field, index) {
                var groupItemValue = tableGroupItemValue(field, index);

                return !$common.isEmptyString(groupItemValue.name) && !$common.isEmptyString(groupItemValue.className);
            };

            $scope.tableGroupItemSave = function (field, groupIndex, index) {
                var groupItemValue = tableGroupItemValue(field, index);

                if (!$common.isValidJavaClass('Group field', groupItemValue.className, true))
                    return focusInvalidField(index, 'ClassName');

                var fields = $scope.backupItem.groups[groupIndex].fields;

                if ($common.isDefined(fields)) {
                    var idx = _.findIndex(fields, function (field) {
                        return field.name == groupItemValue.name;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index) {
                        $common.showError('Field with such name already exists in group!');

                        return focusInvalidField(index, 'FieldName');
                    }
                }

                var group = $scope.backupItem.groups[groupIndex];

                if (index < 0) {
                    if (group.fields)
                        group.fields.push(groupItemValue);
                    else
                        group.fields = [groupItemValue];

                    $scope.tableGroupNewItem(field, groupIndex);
                }
                else {
                    var groupItem = group.fields[index];

                    groupItem.name = groupItemValue.name;
                    groupItem.className = groupItemValue.className;
                    groupItem.direction = groupItemValue.direction;

                    if (index < group.fields.length - 1)
                        $scope.tableGroupItemStartEdit(field, groupIndex, index + 1);
                    else
                        $scope.tableGroupNewItem(field, groupIndex);
                }
            };

            $scope.tableRemoveGroupItem = function (group, index) {
                $table.tableReset();

                group.fields.splice(index, 1);
            };
        }]
);
