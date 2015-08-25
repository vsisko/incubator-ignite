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

var controlCenterModule = angular.module('ignite-web-control-center', ['smart-table', 'mgcrea.ngStrap', 'ui.ace', 'ngSanitize']);

// Modal popup configuration.
controlCenterModule.config(function ($modalProvider) {
    angular.extend($modalProvider.defaults, {
        html: true
    });
});

// Comboboxes configuration.
controlCenterModule.config(function ($popoverProvider) {
    angular.extend($popoverProvider.defaults, {
        trigger: 'manual',
        placement: 'right',
        container: 'body',
        templateUrl: '/popover'
    });
});

// Tooltips configuration.
controlCenterModule.config(function ($tooltipProvider) {
    angular.extend($tooltipProvider.defaults, {
        container: 'body',
        placement: 'right',
        html: 'true',
        trigger: 'click hover'
    });
});

// Comboboxes configuration.
controlCenterModule.config(function ($selectProvider) {
    angular.extend($selectProvider.defaults, {
        maxLength: '1',
        allText: 'Select All',
        noneText: 'Clear All',
        templateUrl: '/select',
        iconCheckmark: 'fa fa-check',
        caretHtml: '<span class="caret"></span>'
    });
});

// Alerts configuration.
controlCenterModule.config(function ($alertProvider) {
    angular.extend($alertProvider.defaults, {
        container: 'body',
        placement: 'top-right',
        duration: '5',
        type: 'danger'
    });
});

// Common functions to be used in controllers.
controlCenterModule.service('$common', [
    '$alert', '$popover', '$timeout', '$focus', function ($alert, $popover, $timeout, $focus) {
        function isDefined(v) {
            return !(v === undefined || v === null);
        }

        function isEmptyArray(arr) {
            if (isDefined(arr))
                return arr.length == 0;

            return true;
        }

        function isEmptyString(s) {
            if (isDefined(s))
                return s.trim().length == 0;

            return true;
        }

        var msgModal = undefined;

        function errorMessage(errMsg) {
            return errMsg ? errMsg : 'Internal server error.';
        }

        function showError(msg, placement, container) {
            if (msgModal)
                msgModal.hide();

            msgModal = $alert({
                title: errorMessage(msg),
                placement: placement ? placement : 'top-right',
                container: container ? container : 'body'
            });

            return false;
        }

        var javaBuildInClasses = [
            'BigDecimal', 'Boolean', 'Byte', 'Date', 'Double', 'Float', 'Integer', 'Long', 'Short', 'String', 'Time', 'Timestamp', 'UUID'
        ];

        var javaBuildInFullNameClasses = [
            'java.math.BigDecimal', 'java.lang.Boolean', 'java.lang.Byte', 'java.sql.Date', 'java.lang.Double',
            'java.lang.Float', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Short', 'java.lang.String',
            'java.sql.Time', 'java.sql.Timestamp', 'java.util.UUID'
        ];

        function isJavaBuildInClass(cls) {
            if (isEmptyString(cls))
                return false;

            return _.contains(javaBuildInClasses, cls) || _.contains(javaBuildInFullNameClasses, cls);
        }

        var SUPPORTED_JDBC_TYPES = [
            'BIT', 'BOOLEAN', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'REAL', 'FLOAT', 'DOUBLE',
            'NUMERIC', 'DECIMAL', 'CHAR', 'VARCHAR', 'LONGVARCHAR', 'NCHAR', 'NVARCHAR', 'LONGNVARCHAR',
            'DATE', 'TIME', 'TIMESTAMP'
        ];

        var ALL_JDBC_TYPES = [
            {dbName: 'BIT', dbType: -7, javaType: 'Boolean'},
            {dbName: 'TINYINT', dbType: -6, javaType: 'Byte'},
            {dbName: 'SMALLINT', dbType:  5, javaType: 'Short'},
            {dbName: 'INTEGER', dbType: 4, javaType: 'Integer'},
            {dbName: 'BIGINT', dbType: -5, javaType: 'Long'},
            {dbName: 'FLOAT', dbType: 6, javaType: 'Float'},
            {dbName: 'REAL', dbType: 7, javaType: 'Double'},
            {dbName: 'DOUBLE', dbType: 8, javaType: 'Double'},
            {dbName: 'NUMERIC', dbType: 2, javaType: 'BigDecimal'},
            {dbName: 'DECIMAL', dbType: 3, javaType: 'BigDecimal'},
            {dbName: 'CHAR', dbType: 1, javaType: 'String'},
            {dbName: 'VARCHAR', dbType: 12, javaType: 'String'},
            {dbName: 'LONGVARCHAR', dbType: -1, javaType: 'String'},
            {dbName: 'DATE', dbType: 91, javaType: 'Date'},
            {dbName: 'TIME', dbType: 92, javaType: 'Time'},
            {dbName: 'TIMESTAMP', dbType: 93, javaType: 'Timestamp'},
            {dbName: 'BINARY', dbType: -2, javaType: 'Object'},
            {dbName: 'VARBINARY', dbType: -3, javaType: 'Object'},
            {dbName: 'LONGVARBINARY', dbType: -4, javaType: 'Object'},
            {dbName: 'NULL', dbType: 0, javaType: 'Object'},
            {dbName: 'OTHER', dbType: 1111, javaType: 'Object'},
            {dbName: 'JAVA_OBJECT', dbType: 2000, javaType: 'Object'},
            {dbName: 'DISTINCT', dbType: 2001, javaType: 'Object'},
            {dbName: 'STRUCT', dbType: 2002, javaType: 'Object'},
            {dbName: 'ARRAY', dbType: 2003, javaType: 'Object'},
            {dbName: 'BLOB', dbType: 2004, javaType: 'Object'},
            {dbName: 'CLOB', dbType: 2005, javaType: 'String'},
            {dbName: 'REF', dbType: 2006, javaType: 'Object'},
            {dbName: 'DATALINK', dbType: 70, javaType: 'Object'},
            {dbName: 'BOOLEAN', dbType: 16, javaType: 'Boolean'},
            {dbName: 'ROWID', dbType: -8, javaType: 'Object'},
            {dbName: 'NCHAR', dbType: -15, javaType: 'String'},
            {dbName: 'NVARCHAR', dbType: -9, javaType: 'String'},
            {dbName: 'LONGNVARCHAR', dbType: -16, javaType: 'String'},
            {dbName: 'NCLOB', dbType: 2011, javaType: 'String'},
            {dbName: 'SQLXML', dbType: 2009, javaType: 'Object'}
        ];

        var JAVA_KEYWORDS = [
            'abstract',     'assert',        'boolean',      'break',           'byte',
            'case',         'catch',         'char',         'class',           'const',
            'continue',     'default',       'do',           'double',          'else',
            'enum',         'extends',       'false',        'final',           'finally',
            'float',        'for',           'goto',         'if',              'implements',
            'import',       'instanceof',    'int',          'interface',       'long',
            'native',       'new',           'null',         'package',         'private',
            'protected',    'public',        'return',       'short',           'static',
            'strictfp',     'super',         'switch',       'synchronized',    'this',
            'throw',        'throws',        'transient',    'true',            'try',
            'void',         'volatile',      'while'
        ];

        var VALID_JAVA_IDENTIFIER = new RegExp('^[a-zA-Z_$][a-zA-Z\d_$]*');

        function isValidJavaIdentifier(msg, ident, elemId) {
            if (isEmptyString(ident))
                return showPopoverMessage(null, null, elemId, msg + ' could not be empty!');

            if (_.contains(JAVA_KEYWORDS, ident))
                return showPopoverMessage(null, null, elemId, msg + ' could not contains reserved java keyword: "' + ident + '"!');

            if (!VALID_JAVA_IDENTIFIER.test(ident))
                return showPopoverMessage(null, null, elemId, msg + ' contains invalid identifier: "' + ident + '"!');

            return true;
        }

        var context = null;

        /**
         * Calculate width of specified text in body's font.
         *
         * @param text Text to calculate width.
         * @returns {Number} Width of text in pixels.
         */
        function measureText(text) {
            if (!context) {
                var canvas = document.createElement('canvas');

                context = canvas.getContext('2d');

                var style = window.getComputedStyle(document.getElementsByTagName('body')[0]);

                context.font = style.fontSize + ' ' + style.fontFamily;
            }

            return context.measureText(text).width;
        }

        /**
         * Compact java full class name by max number of characters.
         *
         * @param names Array of class names to compact.
         * @param nameLength Max available width in characters for simple name.
         * @returns {*} Array of compacted class names.
         */
        function compactByMaxCharts(names, nameLength) {
            for (var nameIx = 0; nameIx < names.length; nameIx ++) {
                var s = names[nameIx];

                if (s.length > nameLength) {
                    var totalLength = s.length;

                    var packages = s.split('.');

                    var packageCnt = packages.length - 1;

                    for (var i = 0; i < packageCnt && totalLength > nameLength; i++) {
                        if (packages[i].length > 0) {
                            totalLength -= packages[i].length - 1;

                            packages[i] = packages[i][0];
                        }
                    }

                    if (totalLength > nameLength) {
                        var className = packages[packageCnt];

                        var classNameLen = className.length;

                        var remains = Math.min(nameLength - totalLength + classNameLen, classNameLen);

                        if (remains < 3)
                            remains = Math.min(3, classNameLen);

                        packages[packageCnt] = className.substring(0, remains) + '...';
                    }

                    var result = packages[0];

                    for (i = 1; i < packages.length; i++)
                        result += '.' + packages[i];

                    names[nameIx] = result;
                }
            }

            return names
        }

        /**
         * Compact java full class name by max number of pixels.
         *
         * @param names Array of class names to compact.
         * @param nameLength Max available width in characters for simple name. Used for calculation optimization.
         * @param nameWidth Maximum available width in pixels for simple name.
         * @returns {*} Array of compacted class names.
         */
        function compactByMaxPixels(names, nameLength, nameWidth) {
            if (nameWidth <= 0)
                return names;

            var fitted = [];

            var widthByName = [];

            var len = names.length;

            var divideTo = len;

            for (var nameIx = 0; nameIx < len; nameIx ++) {
                fitted[nameIx] = false;

                widthByName[nameIx] = nameWidth;
            }

            // Try to distribute space from short class names to long class names.
            do {
                var remains = 0;

                for (nameIx = 0; nameIx < len; nameIx++) {
                    if (!fitted[nameIx]) {
                        var curNameWidth = measureText(names[nameIx]);

                        if (widthByName[nameIx] > curNameWidth) {
                            fitted[nameIx] = true;

                            remains += widthByName[nameIx] - curNameWidth;

                            divideTo -= 1;

                            widthByName[nameIx] = curNameWidth;
                        }
                    }
                }

                var remainsByName = remains / divideTo;

                for (nameIx = 0; nameIx < len; nameIx++) {
                    if (!fitted[nameIx]) {
                        widthByName[nameIx] += remainsByName;
                    }
                }
            }
            while(remains > 0);

            // Compact class names to available for each space.
            for (nameIx = 0; nameIx < len; nameIx ++) {
                var s = names[nameIx];

                if (s.length > (nameLength / 2) | 0) {
                    var totalWidth = measureText(s);

                    if (totalWidth > widthByName[nameIx]) {
                        var packages = s.split('.');

                        var packageCnt = packages.length - 1;

                        for (var i = 0; i < packageCnt && totalWidth > widthByName[nameIx]; i++) {
                            if (packages[i].length > 1) {
                                totalWidth -= measureText(packages[i].substring(1, packages[i].length));

                                packages[i] = packages[i][0];
                            }
                        }

                        var shortPackage = '';

                        for (i = 0; i < packageCnt; i++)
                            shortPackage += packages[i] + '.';

                        var className = packages[packageCnt];

                        var classLen = className.length;

                        var minLen = Math.min(classLen, 3);

                        totalWidth = measureText(shortPackage + className);

                        // Compact class name if shorten package path is very long.
                        if (totalWidth > widthByName[nameIx]) {
                            var maxLen = classLen;

                            var middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;

                            var minLenPx = measureText(shortPackage + className.substr(0, minLen) + '...');
                            var maxLenPx = totalWidth;

                            while (middleLen != minLen && middleLen != maxLen) {
                                var middleLenPx = measureText(shortPackage + className.substr(0, middleLen) + '...');

                                if (middleLenPx > widthByName[nameIx]) {
                                    maxLen = middleLen;
                                    maxLenPx = middleLenPx;
                                }
                                else {
                                    minLen = middleLen;
                                    minLenPx = middleLenPx;
                                }

                                middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;
                            }

                            names[nameIx] = shortPackage + className.substring(0, middleLen) + '...';
                        }
                        else
                            names[nameIx] = shortPackage + className;
                    }
                }
            }

            return names;
        }

        /**
         * Calculate available width for text in link to edit element.
         *
         * @param index Showed index of element for calcuraion maximal width in pixel.
         * @param id Id of contains link table.
         * @returns {*[]} First element is length of class for single value, second element is length for pair vlaue.
         */
        function availableWidth(index, id) {
            var divs = $($('#' + id).find('tr')[index - 1]).find('div');

            var div = null;

            var width = 0;

            for (var divIx = 0; divIx < divs.length; divIx ++)
                if (divs[divIx].className.length == 0 && (div == null ||  divs[divIx].childNodes.length > div.childNodes.length))
                    div = divs[divIx];

            if (div != null) {
                width = div.clientWidth;

                if (width > 0) {
                    var children = div.childNodes;

                    for (var i = 1; i < children.length; i++) {
                        var child = children[i];

                        if ('offsetWidth' in child)
                            width -= $(children[i]).outerWidth(true);
                    }
                }
            }

            return width | 0;
        }

        var popover = null;

        function ensureActivePanel(panels, id) {
            if (panels) {
                var idx = _.findIndex($('div.panel-collapse'), function(pnl) {
                    return pnl.id == id;
                });

                if (idx >= 0) {
                    var activePanels = panels.activePanels;

                    if (!activePanels || activePanels.length < 1)
                        panels.activePanels = [idx];
                    else if (!_.contains(activePanels, idx)) {
                        var newActivePanels = activePanels.slice();

                        newActivePanels.push(idx);

                        panels.activePanels = newActivePanels;
                    }
                }
            }
        }

        function showPopoverMessage(panels, panelId, id, message) {
            ensureActivePanel(panels, panelId);

            var el = $('body').find('#' + id);

            if (popover)
                popover.hide();

            var newPopover = $popover(el, {content: message});

            $timeout(function () {
                $focus(id);
            }, 50);

            $timeout(function () {
                newPopover.show();

                popover = newPopover;
            }, 100);

            $timeout(function () { newPopover.hide() }, 3000);

            return false;
        }

        var win = $(window);

        var elem = undefined;
        var offsetTop = undefined;
        var cont = undefined;
        var scrollspyWidth = 0

        win.scroll(function() {
            if (!isDefined(offsetTop)) {
                elem = $('#scrolled')

                if (elem.length > 0) {
                    cont = $('.docs-content');

                    offsetTop = elem.offset().top;

                    scrollspyWidth = elem[0].getBoundingClientRect().width

                    console.log(scrollspyWidth)
                }
            }
            else {
                elem.toggleClass('panel-sticky', win.scrollTop() > offsetTop);

                elem.width(scrollspyWidth)
            }
        });

        return {
            getModel: function (obj, field) {
                var path = field.path;

                if (!isDefined(path))
                    return obj;

                path = path.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
                path = path.replace(/^\./, '');           // strip a leading dot

                var segs = path.split('.');
                var root = obj;

                while (segs.length > 0) {
                    var pathStep = segs.shift();

                    if (typeof root[pathStep] === 'undefined')
                        root[pathStep] = {};

                    root = root[pathStep];
                }

                return root;
            },
            joinTip: function (arr) {
                if (!arr) {
                    return arr;
                }

                var lines = arr.map(function (line) {
                    var rtrimmed = line.replace(/\s+$/g, '');

                    if (rtrimmed.indexOf('>', this.length - 1) == -1) {
                        rtrimmed = rtrimmed + '<br/>';
                    }

                    return rtrimmed;
                });

                return lines.join('');
            },
            mkOptions: function (options) {
                return _.map(options, function (option) {
                    return {value: option, label: isDefined(option) ? option : 'Not set'};
                });
            },
            isDefined: isDefined,
            hasProperty: function (obj, props) {
                for (var propName in props) {
                    if (props.hasOwnProperty(propName)) {
                        if (obj[propName])
                            return true;
                    }
                }

                return false;
            },
        isEmptyArray: isEmptyArray,
            isEmptyString: isEmptyString,
            errorMessage: errorMessage,
            showError: showError,
            showInfo: function (msg) {
                if (msgModal)
                    msgModal.hide();

                msgModal = $alert({
                    type: 'success',
                    title: msg,
                    duration: 2
                });
            },
            SUPPORTED_JDBC_TYPES: SUPPORTED_JDBC_TYPES,
            findJdbcType: function (jdbcType) {
                var res =  _.find(ALL_JDBC_TYPES, function (item) {
                    return item.dbType == jdbcType;
                });

                return res ? res : {dbName: 'Unknown', javaType: 'Unknown'}
            },
            javaBuildInClasses: javaBuildInClasses,
            isJavaBuildInClass: isJavaBuildInClass,
            isValidJavaIdentifier: isValidJavaIdentifier,
            isValidJavaClass: function (msg, ident, allowBuildInClass, elemId) {
                if (isEmptyString(ident))
                    return showPopoverMessage(null, null, elemId, msg + ' could not be empty!');

                var parts = ident.split('.');

                var len = parts.length;

                if (!allowBuildInClass && isJavaBuildInClass(ident))
                    return showPopoverMessage(null, null, elemId, msg + ' should not be the Java build-in class!');

                if (len < 2 && !isJavaBuildInClass(ident))
                    return showPopoverMessage(null, null, elemId, msg + ' does not have package specified!');

                for (var i = 0; i < parts.length; i++) {
                    var part = parts[i];

                    if (!isValidJavaIdentifier(msg, part))
                        return false;
                }

                return true;
            },
            /**
             * Cut class name by width in pixel or width in symbol count.
             *
             * @param id Id of contains link table.
             * @param index Showed index of element.
             * @param maxLength Maximum length in symbols for all names.
             * @param names Array of class names to compact.
             * @returns {*} Array of compacted class names.
             */
            compactJavaName: function (id, index, maxLength, names) {
                var prefix = index + ') ';

                var nameCnt = names.length;

                var nameLength = ((maxLength - 3 * (nameCnt - 1)) / nameCnt) | 0;

                try {
                    var nameWidth = (availableWidth(index, id) - measureText(prefix) - (nameCnt - 1) * measureText(' / ')) /
                        nameCnt | 0;

                    // HTML5 calculation of showed message width.
                    names = compactByMaxPixels(names, nameLength, nameWidth);
                }
                catch (err) {
                    names = compactByMaxCharts(names, nameLength);
                }

                var result = prefix + names[0];

                for (var nameIx = 1; nameIx < names.length; nameIx ++)
                    result += ' / ' + names[nameIx];

                return result;
            },
            ensureActivePanel: function (panels, id) {
                ensureActivePanel(panels, id);
            },
            showPopoverMessage: function (panels, panelId, id, message) {
                return showPopoverMessage(panels, panelId, id, message)
            },
            hidePopover: function () {
                if (popover)
                    popover.hide();
            }
        }
    }]);

// Confirm popup service.
controlCenterModule.service('$confirm', function ($modal, $rootScope, $q) {
    var scope = $rootScope.$new();

    var deferred;

    scope.ok = function () {
        deferred.resolve();

        confirmModal.hide();
    };

    var confirmModal = $modal({templateUrl: '/confirm', scope: scope, placement: 'center', show: false});

    var parentShow = confirmModal.show;

    confirmModal.show = function (content) {
        scope.content = content || 'Confirm deletion?';

        deferred = $q.defer();

        parentShow();

        return deferred.promise;
    };

    return confirmModal;
});

// 'Save as' popup service.
controlCenterModule.service('$copy', function ($modal, $rootScope, $q) {
    var scope = $rootScope.$new();

    var deferred;

    scope.ok = function (newName) {
        deferred.resolve(newName);

        copyModal.hide();
    };

    var copyModal = $modal({templateUrl: '/copy', scope: scope, placement: 'center', show: false});

    var parentShow = copyModal.show;

    copyModal.show = function (oldName) {
        scope.newName = oldName + '(1)';

        deferred = $q.defer();

        parentShow();

        return deferred.promise;
    };

    return copyModal;
});

// Tables support service.
controlCenterModule.service('$table', ['$common', '$focus', function ($common, $focus) {
    function _swapSimpleItems(a, ix1, ix2) {
        var tmp = a[ix1];

        a[ix1] = a[ix2];
        a[ix2] = tmp;
    }

    function _model(item, field) {
        return $common.getModel(item, field);
    }

    var table = {name: 'none', editIndex: -1};

    function _tableReset() {
        table.name = 'none';
        table.editIndex = -1;

        $common.hidePopover();
    }

    function _tableState(name, editIndex) {
        table.name = name;
        table.editIndex = editIndex;
    }

    function _tableUI(field) {
        var ui = field.ui;

        return ui ? ui : field.type;
    }

    function _tableFocus(focusId, index) {
        $focus((index < 0 ? 'new' : 'cur') + focusId);
    }

    function _tableSimpleValue(filed, index) {
        return index < 0 ? filed.newValue : filed.curValue;
    }

    function _tablePairValue(filed, index) {
        return index < 0 ? {key: filed.newKey, value: filed.newValue} : {key: filed.curKey, value: filed.curValue};
    }

    function _tableStartEdit(item, field, index) {
        _tableState(field.model, index);

        var val = _model(item, field)[field.model][index];

        var ui = _tableUI(field);

        if (ui == 'table-simple') {
            field.curValue = val;

            _tableFocus(field.focusId, index);
        }
        else if (ui == 'table-pair') {
            field.curKey = val[field.keyName];
            field.curValue = val[field.valueName];

            _tableFocus('Key' + field.focusId, index);
        }
        else if (ui == 'table-db-fields') {
            field.curDatabaseName = val.databaseName;
            field.curDatabaseType = val.databaseType;
            field.curJavaName = val.javaName;
            field.curJavaType = val.javaType;

            _tableFocus('DatabaseName' + field.focusId, index);
        }
        else if (ui == 'table-query-groups') {
            field.curGroupName = val.name;
            field.curFields = val.fields;

            _tableFocus('GroupName', index);
        }
    }

    function _tableNewItem(field) {
        _tableState(field.model, -1);

        var ui = _tableUI(field);

        if (ui == 'table-simple') {
            field.newValue = null;

            _tableFocus(field.focusId, -1);
        }
        else if (ui == 'table-pair') {
            field.newKey = null;
            field.newValue = null;

            _tableFocus('Key' + field.focusId, -1);
        }
        else if (ui == 'table-db-fields') {
            field.newDatabaseName = null;
            field.newDatabaseType = 'INTEGER';
            field.newJavaName = null;
            field.newJavaType = 'Integer';

            _tableFocus('DatabaseName' + field.focusId, -1);
        }
        else if (ui == 'table-query-groups') {
            field.newGroupName = null;
            field.newFields = null;

            _tableFocus('GroupName', -1);
        }
        else if (ui == 'table-query-group-fields') {
            _tableFocus('FieldName', -1);
        }
    }

    return {
        tableState: function (name, editIndex) {
            _tableState(name, editIndex);
        },
        tableReset: function () {
            _tableReset();
        },
        tableNewItem: _tableNewItem,
        tableNewItemActive: function (field) {
            return table.name == field.model && table.editIndex < 0;
        },
        tableEditing: function (field, index) {
            return table.name == field.model && table.editIndex == index;
        },
        tableStartEdit: _tableStartEdit,
        tableRemove: function (item, field, index) {
            _tableReset();

            _model(item, field)[field.model].splice(index, 1);
        },
        tableSimpleSave: function (valueValid, item, field, index) {
            var simpleValue = _tableSimpleValue(field, index);

            if (valueValid(item, field, simpleValue, index)) {
                _tableReset();

                if (index < 0) {
                    if (_model(item, field)[field.model])
                        _model(item, field)[field.model].push(simpleValue);
                    else
                        _model(item, field)[field.model] = [simpleValue];

                    _tableNewItem(field);
                }
                else {
                    var arr = _model(item, field)[field.model];

                    arr[index] = simpleValue;

                    if (index < arr.length - 1)
                        _tableStartEdit(item, field, index + 1);
                    else
                        _tableNewItem(field);
                }
            }
        },
        tableSimpleSaveVisible: function (field, index) {
            return !$common.isEmptyString(_tableSimpleValue(field, index));
        },
        tableSimpleUp: function (item, field, index) {
            _tableReset();

            _swapSimpleItems(_model(item, field)[field.model], index, index - 1);
        },
        tableSimpleDown: function (item, field, index) {
            _tableReset();

            _swapSimpleItems(_model(item, field)[field.model], index, index + 1);
        },
        tableSimpleDownVisible: function (item, field, index) {
            return index < _model(item, field)[field.model].length - 1;
        },
        tablePairValue: _tablePairValue,
        tablePairSave: function (pairValid, item, field, index) {
            if (pairValid(item, field, index)) {
                var pairValue = _tablePairValue(field, index);

                var pairModel = {};

                if (index < 0) {
                    pairModel[field.keyName] = pairValue.key;
                    pairModel[field.valueName] = pairValue.value;

                    if (item[field.model])
                        item[field.model].push(pairModel);
                    else
                        item[field.model] = [pairModel];

                    _tableNewItem(field);
                }
                else {
                    pairModel = item[field.model][index];

                    pairModel[field.keyName] = pairValue.key;
                    pairModel[field.valueName] = pairValue.value;

                    if (index < item[field.model].length - 1)
                        _tableStartEdit(item, field, index + 1);
                    else
                        _tableNewItem(field);
                }
            }
        },
        tablePairSaveVisible: function (field, index) {
            var pairValue = _tablePairValue(field, index);

            return !$common.isEmptyString(pairValue.key) && !$common.isEmptyString(pairValue.value);
        },
        tableFocusInvalidField: function (index, id) {
            _tableFocus(id, index);

            return false;
        },
        tableFieldId: function (index, id) {
            return (index < 0 ? 'new' : 'cur') + id;
        }
    }
}]);


// Preview support service.
controlCenterModule.service('$preview', [function () {
    return {
        previewInit: function (editor) {
            editor.setReadOnly(true);
            editor.setOption("highlightActiveLine", false);
            editor.$blockScrolling = Infinity;

            var renderer = editor.renderer;

            renderer.setHighlightGutterLine(false);
            renderer.setShowPrintMargin(false);
            renderer.setOption('fontSize', '10px');

            editor.setTheme('ace/theme/chrome');
        }
    }
}]);

// Preview support service.
controlCenterModule.service('$code', ['$common', function ($common) {
    function builder() {
        var res = [];

        res.deep = 0;
        res.lineStart = true;

        res.append = function (s) {
            if (this.lineStart) {
                for (var i = 0; i < this.deep; i++)
                    this.push('    ');

                this.lineStart = false;
            }

            this.push(s);

            return this;
        };

        res.line = function (s) {
            if (s)
                this.append(s);

            this.push('\n');
            this.lineStart = true;

            return this;
        };

        res.startBlock = function (s) {
            if (s)
                this.append(s);

            this.push('\n');
            this.lineStart = true;
            this.deep++;

            return this;
        };

        res.endBlock = function (s) {
            this.deep--;

            if (s)
                this.append(s);

            this.push('\n');
            this.lineStart = true;

            return this;
        };

        res.emptyLineIfNeeded = function () {
            if (this.needEmptyLine) {
                this.line();

                this.needEmptyLine = false;

                return true;
            }

            return false;
        };

        res.imports = {};

        res.importClass = function (className) {
            var fullClassName = javaBuildInClass(className);

            var dotIdx = fullClassName.lastIndexOf('.');

            var shortName = dotIdx > 0 ? fullClassName.substr(dotIdx + 1) : fullClassName;

            if (this.imports[shortName]) {
                if (this.imports[shortName] != fullClassName)
                    return fullClassName; // Short class names conflict. Return full name.
            }
            else
                this.imports[shortName] = fullClassName;

            return shortName;
        };

        /**
         * @returns String with "java imports" section.
         */
        res.generateImports = function () {
            var res = [];

            for (var clsName in this.imports) {
                if (this.imports.hasOwnProperty(clsName) && this.imports[clsName].lastIndexOf('java.lang.', 0) != 0)
                    res.push('import ' + this.imports[clsName] + ';');
            }

            res.sort();

            return res.join('\n')
        };

        return res;
    }

    function escapeAttr(s) {
        if (typeof(s) != 'string')
            return s;

        return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
    }

    function addElement(res, tag, attr1, val1, attr2, val2) {
        var elem = '<' + tag;

        if (attr1) {
            elem += ' ' + attr1 + '="' + val1 + '"'
        }

        if (attr2) {
            elem += ' ' + attr2 + '="' + val2 + '"'
        }

        elem += '/>';

        res.emptyLineIfNeeded();
        res.line(elem);
    }

    function addProperty(res, obj, propName, setterName) {
        var val = obj[propName];

        if ($common.isDefined(val))
            addElement(res, 'property', 'name', setterName ? setterName : propName, 'value', escapeAttr(val));

        return val;
    }

    function addClassNameProperty(res, obj, propName) {
        var val = obj[propName];

        if ($common.isDefined(val))
            addElement(res, 'property', 'name', propName, 'value', $common.javaBuildInClass(val));

        return val;
    }

    function addListProperty(res, obj, propName, listType, rowFactory) {
        var val = obj[propName];

        if (val && val.length > 0) {
            res.emptyLineIfNeeded();

            if (!listType)
                listType = 'list';

            if (!rowFactory)
                rowFactory = function (val) {
                    return '<value>' + escape(val) + '</value>'
                };

            res.startBlock('<property name="' + propName + '">');
            res.startBlock('<' + listType + '>');

            for (var i = 0; i < val.length; i++)
                res.line(rowFactory(val[i]));

            res.endBlock('</' + listType + '>');
            res.endBlock('</property>');
        }
    }

    function ClassDescriptor(className, fields) {
        this.className = className;
        this.fields = fields;
    }

    var eventGroups = {
        EVTS_CHECKPOINT: ['EVT_CHECKPOINT_SAVED', 'EVT_CHECKPOINT_LOADED', 'EVT_CHECKPOINT_REMOVED'],
        EVTS_DEPLOYMENT: ['EVT_CLASS_DEPLOYED', 'EVT_CLASS_UNDEPLOYED', 'EVT_CLASS_DEPLOY_FAILED', 'EVT_TASK_DEPLOYED',
            'EVT_TASK_UNDEPLOYED', 'EVT_TASK_DEPLOY_FAILED'],
        EVTS_ERROR: ['EVT_JOB_TIMEDOUT', 'EVT_JOB_FAILED', 'EVT_JOB_FAILED_OVER', 'EVT_JOB_REJECTED', 'EVT_JOB_CANCELLED',
            'EVT_TASK_TIMEDOUT', 'EVT_TASK_FAILED', 'EVT_CLASS_DEPLOY_FAILED', 'EVT_TASK_DEPLOY_FAILED',
            'EVT_TASK_DEPLOYED', 'EVT_TASK_UNDEPLOYED', 'EVT_CACHE_REBALANCE_STARTED', 'EVT_CACHE_REBALANCE_STOPPED'],
        EVTS_DISCOVERY: ['EVT_NODE_JOINED', 'EVT_NODE_LEFT', 'EVT_NODE_FAILED', 'EVT_NODE_SEGMENTED',
            'EVT_CLIENT_NODE_DISCONNECTED', 'EVT_CLIENT_NODE_RECONNECTED'],
        EVTS_JOB_EXECUTION: ['EVT_JOB_MAPPED', 'EVT_JOB_RESULTED', 'EVT_JOB_FAILED_OVER', 'EVT_JOB_STARTED',
            'EVT_JOB_FINISHED', 'EVT_JOB_TIMEDOUT', 'EVT_JOB_REJECTED', 'EVT_JOB_FAILED', 'EVT_JOB_QUEUED',
            'EVT_JOB_CANCELLED'],
        EVTS_TASK_EXECUTION: ['EVT_TASK_STARTED', 'EVT_TASK_FINISHED', 'EVT_TASK_FAILED', 'EVT_TASK_TIMEDOUT',
            'EVT_TASK_SESSION_ATTR_SET', 'EVT_TASK_REDUCED'],
        EVTS_CACHE: ['EVT_CACHE_ENTRY_CREATED', 'EVT_CACHE_ENTRY_DESTROYED', 'EVT_CACHE_OBJECT_PUT',
            'EVT_CACHE_OBJECT_READ', 'EVT_CACHE_OBJECT_REMOVED', 'EVT_CACHE_OBJECT_LOCKED', 'EVT_CACHE_OBJECT_UNLOCKED',
            'EVT_CACHE_OBJECT_SWAPPED', 'EVT_CACHE_OBJECT_UNSWAPPED', 'EVT_CACHE_OBJECT_EXPIRED'],
        EVTS_CACHE_REBALANCE: ['EVT_CACHE_REBALANCE_STARTED', 'EVT_CACHE_REBALANCE_STOPPED',
            'EVT_CACHE_REBALANCE_PART_LOADED', 'EVT_CACHE_REBALANCE_PART_UNLOADED', 'EVT_CACHE_REBALANCE_OBJECT_LOADED',
            'EVT_CACHE_REBALANCE_OBJECT_UNLOADED', 'EVT_CACHE_REBALANCE_PART_DATA_LOST'],
        EVTS_CACHE_LIFECYCLE: ['EVT_CACHE_STARTED', 'EVT_CACHE_STOPPED', 'EVT_CACHE_NODES_LEFT'],
        EVTS_CACHE_QUERY: ['EVT_CACHE_QUERY_EXECUTED', 'EVT_CACHE_QUERY_OBJECT_READ'],
        EVTS_SWAPSPACE: ['EVT_SWAP_SPACE_CLEARED', 'EVT_SWAP_SPACE_DATA_REMOVED', 'EVT_SWAP_SPACE_DATA_READ',
            'EVT_SWAP_SPACE_DATA_STORED', 'EVT_SWAP_SPACE_DATA_EVICTED'],
        EVTS_IGFS: ['EVT_IGFS_FILE_CREATED', 'EVT_IGFS_FILE_RENAMED', 'EVT_IGFS_FILE_DELETED', 'EVT_IGFS_FILE_OPENED_READ',
            'EVT_IGFS_FILE_OPENED_WRITE', 'EVT_IGFS_FILE_CLOSED_WRITE', 'EVT_IGFS_FILE_CLOSED_READ', 'EVT_IGFS_FILE_PURGED',
            'EVT_IGFS_META_UPDATED', 'EVT_IGFS_DIR_CREATED', 'EVT_IGFS_DIR_RENAMED', 'EVT_IGFS_DIR_DELETED']
    };

    var atomicConfiguration = new ClassDescriptor('org.apache.ignite.configuration.AtomicConfiguration', {
        backups: null,
        cacheMode: {type: 'enum', enumClass: 'CacheMode'},
        atomicSequenceReserveSize: null
    });

    var evictionPolicies = {
        'LRU': new ClassDescriptor('org.apache.ignite.cache.eviction.lru.LruEvictionPolicy',
            {batchSize: null, maxMemorySize: null, maxSize: null}),
        'RND': new ClassDescriptor('org.apache.ignite.cache.eviction.random.RandomEvictionPolicy', {maxSize: null}),
        'FIFO': new ClassDescriptor('org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy',
            {batchSize: null, maxMemorySize: null, maxSize: null}),
        'SORTED': new ClassDescriptor('org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy',
            {batchSize: null, maxMemorySize: null, maxSize: null})
    };

    var marshallers = {
        OptimizedMarshaller: new ClassDescriptor('org.apache.ignite.marshaller.optimized.OptimizedMarshaller', {
            poolSize: null,
            requireSerializable: null
        }),
        JdkMarshaller: new ClassDescriptor('org.apache.ignite.marshaller.jdk.JdkMarshaller', {})
    };

    var swapSpaceSpi = new ClassDescriptor('org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi', {
        baseDirectory: null,
        readStripesNumber: null,
        maximumSparsity: {type: 'float'},
        maxWriteQueueSize: null,
        writeBufferSize: null
    });

    var transactionConfiguration = new ClassDescriptor('org.apache.ignite.configuration.TransactionConfiguration', {
        defaultTxConcurrency: {type: 'enum', enumClass: 'TransactionConcurrency'},
        transactionIsolation: {type: 'TransactionIsolation', setterName: 'defaultTxIsolation'},
        defaultTxTimeout: null,
        pessimisticTxLogLinger: null,
        pessimisticTxLogSize: null,
        txSerializableEnabled: null
    });

    var knownClasses = {
        Oracle: new ClassDescriptor('org.apache.ignite.cache.store.jdbc.dialect.OracleDialect', {}),
        DB2: new ClassDescriptor('org.apache.ignite.cache.store.jdbc.dialect.DB2Dialect', {}),
        SQLServer: new ClassDescriptor('org.apache.ignite.cache.store.jdbc.dialect.SQLServerDialect', {}),
        MySQL: new ClassDescriptor('org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect', {}),
        PostgreSQL: new ClassDescriptor('org.apache.ignite.cache.store.jdbc.dialect.BasicJdbcDialect', {}),
        H2: new ClassDescriptor('org.apache.ignite.cache.store.jdbc.dialect.H2Dialect', {})
    };

    function addBeanWithProperties(res, bean, beanPropName, beanClass, props, createBeanAlthoughNoProps) {
        if (bean && $common.hasProperty(bean, props)) {
            res.emptyLineIfNeeded();
            res.startBlock('<property name="' + beanPropName + '">');
            res.startBlock('<bean class="' + beanClass + '">');

            for (var propName in props) {
                if (props.hasOwnProperty(propName)) {
                    var descr = props[propName];

                    if (descr) {
                        if (descr.type == 'list') {
                            addListProperty(res, bean, propName, descr.setterName);
                        }
                        else if (descr.type == 'className') {
                            if (bean[propName]) {
                                res.startBlock('<property name="' + propName + '">');
                                res.line('<bean class="' + knownClasses[bean[propName]].className + '"/>');
                                res.endBlock('</property>');
                            }
                        }
                        else if (descr.type == 'propertiesAsList') {
                            var val = bean[propName];

                            if (val && val.length > 0) {
                                res.startBlock('<property name="' + propName + '">');
                                res.startBlock('<props>');

                                for (var i = 0; i < val.length; i++) {
                                    var nameAndValue = val[i];

                                    var eqIndex = nameAndValue.indexOf('=');
                                    if (eqIndex >= 0) {
                                        res.line('<prop key="' + escapeAttr(nameAndValue.substring(0, eqIndex)) + '">' +
                                            escape(nameAndValue.substr(eqIndex + 1)) + '</prop>');
                                    }
                                }

                                res.endBlock('</props>');
                                res.endBlock('</property>');
                            }
                        }
                        else
                            addProperty(res, bean, propName, descr.setterName);
                    }
                    else
                        addProperty(res, bean, propName);
                }
            }

            res.endBlock('</bean>');
            res.endBlock('</property>');
        }
        else if (createBeanAlthoughNoProps) {
            res.emptyLineIfNeeded();
            res.line('<property name="' + beanPropName + '">');
            res.line('    <bean class="' + beanClass + '"/>');
            res.line('</property>');
        }
    }

    return {
        xmlClusterGeneral: function (cluster, res) {
            if (!res)
                res = builder();

            // Generate discovery.
            if (cluster.discovery) {
                res.startBlock('<property name="discoverySpi">');
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">');
                res.startBlock('<property name="ipFinder">');

                var d = cluster.discovery;

                switch (d.kind) {
                    case 'Multicast':
                        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder">');

                        addProperty(res, d.Multicast, 'multicastGroup');
                        addProperty(res, d.Multicast, 'multicastPort');
                        addProperty(res, d.Multicast, 'responseWaitTime');
                        addProperty(res, d.Multicast, 'addressRequestAttempts');
                        addProperty(res, d.Multicast, 'localAddress');

                        res.endBlock('</bean>');

                        break;

                    case 'Vm':
                        if (d.Vm.addresses.length > 0) {
                            res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">');

                            addListProperty(res, d.Vm, 'addresses');

                            res.endBlock('</bean>');
                        }
                        else {
                            res.line('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder"/>');
                        }

                        break;

                    case 'S3':
                        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder">');

                        if (d.S3 && d.S3.bucketName)
                            res.line('<property name="bucketName" value="' + escapeAttr(d.S3.bucketName) + '" />');

                        res.endBlock('</bean>');

                        break;

                    case 'Cloud':
                        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder">');

                        addProperty(res, d.Cloud, 'credential');
                        addProperty(res, d.Cloud, 'credentialPath');
                        addProperty(res, d.Cloud, 'identity');
                        addProperty(res, d.Cloud, 'provider');
                        addListProperty(res, d.Cloud, 'regions');
                        addListProperty(res, d.Cloud, 'zones');

                        res.endBlock('</bean>');

                        break;

                    case 'GoogleStorage':
                        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder">');

                        addProperty(res, d.GoogleStorage, 'projectName');
                        addProperty(res, d.GoogleStorage, 'bucketName');
                        addProperty(res, d.GoogleStorage, 'serviceAccountP12FilePath');
                        addProperty(res, d.GoogleStorage, 'serviceAccountId');

                        //if (d.GoogleStorage.addrReqAttempts) todo ????
                        //    res.line('<property name="serviceAccountP12FilePath" value="' + escapeAttr(d.GoogleStorage.addrReqAttempts) + '"/>');

                        res.endBlock('</bean>');

                        break;

                    case 'Jdbc':
                        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder">');
                        res.line('<property name="initSchema" value="' + ($common.isDefined(d.Jdbc.initSchema) && d.Jdbc.initSchema) + '"/>');
                        res.endBlock('</bean>');

                        break;

                    case 'SharedFs':
                        if (d.SharedFs.path) {
                            res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder">');
                            addProperty(res, d.SharedFs, 'path');
                            res.endBlock('</bean>');
                        }
                        else {
                            res.line('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder"/>');
                        }

                        break;

                    default:
                        throw "Unknown discovery kind: " + d.kind;
                }

                res.endBlock('</property>');
                res.endBlock('</bean>');
                res.endBlock('</property>');

                res.needEmptyLine = true;
            }

            return res;
        },
        xmlClusterAtomics: function (cluster, res) {
            if (!res)
                res = builder();

            // Generate atomics group.
            addBeanWithProperties(res, cluster.atomicConfiguration, 'atomicConfiguration',
                atomicConfiguration.className, atomicConfiguration.fields);
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterCommunication: function (cluster, res) {
            if (!res)
                res = builder();

            // Generate communication group.
            addProperty(res, cluster, 'networkTimeout');
            addProperty(res, cluster, 'networkSendRetryDelay');
            addProperty(res, cluster, 'networkSendRetryCount');
            addProperty(res, cluster, 'segmentCheckFrequency');
            addProperty(res, cluster, 'waitForSegmentOnStart');
            addProperty(res, cluster, 'discoveryStartupDelay');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterDeployment: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate deployment group.
            addProperty(res, cluster, 'deploymentMode');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterEvents: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate events group.
            if (cluster.includeEventTypes && cluster.includeEventTypes.length > 0) {
                res.emptyLineIfNeeded();

                res.startBlock('<property name="includeEventTypes">');

                if (cluster.includeEventTypes.length == 1)
                    res.line('<util:constant static-field="org.apache.ignite.events.EventType.' + cluster.includeEventTypes[0] + '"/>');
                else {
                    res.startBlock('<array>');

                    for (i = 0; i < cluster.includeEventTypes.length; i++) {
                        if (i > 0)
                            res.line();

                        var eventGroup = cluster.includeEventTypes[i];

                        res.line('<!-- EventType.' + eventGroup + ' -->');

                        var eventList = eventGroups[eventGroup];

                        for (var k = 0; k < eventList.length; k++) {
                            res.line('<util:constant static-field="org.apache.ignite.events.EventType.' + eventList[k] + '"/>')
                        }
                    }

                    res.endBlock('</array>');
                }

                res.endBlock('</property>');

                res.needEmptyLine = true;
            }

            return res;
        },
        xmlClusterMarshaller: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate marshaller group.
            var marshaller = cluster.marshaller;

            if (marshaller && marshaller.kind) {
                var marshallerDesc = marshallers[marshaller.kind];

                addBeanWithProperties(res, marshaller[marshaller.kind], 'marshaller', marshallerDesc.className, marshallerDesc.fields, true);
                res.needEmptyLine = true;
            }

            addProperty(res, cluster, 'marshalLocalJobs');
            addProperty(res, cluster, 'marshallerCacheKeepAliveTime');
            addProperty(res, cluster, 'marshallerCacheThreadPoolSize');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterMetrics: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate metrics group.
            addProperty(res, cluster, 'metricsExpireTime');
            addProperty(res, cluster, 'metricsHistorySize');
            addProperty(res, cluster, 'metricsLogFrequency');
            addProperty(res, cluster, 'metricsUpdateFrequency');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterP2P: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate PeerClassLoading group.
            addProperty(res, cluster, 'peerClassLoadingEnabled');
            addListProperty(res, cluster, 'peerClassLoadingLocalClassPathExclude');
            addProperty(res, cluster, 'peerClassLoadingMissedResourcesCacheSize');
            addProperty(res, cluster, 'peerClassLoadingThreadPoolSize');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterSwap: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate swap group.
            if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
                addBeanWithProperties(res, cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi',
                    swapSpaceSpi.className, swapSpaceSpi.fields, true);

                res.needEmptyLine = true;
            }

            return res;
        },
        xmlClusterTime: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate time group.
            addProperty(res, cluster, 'clockSyncSamples');
            addProperty(res, cluster, 'clockSyncFrequency');
            addProperty(res, cluster, 'timeServerPortBase');
            addProperty(res, cluster, 'timeServerPortRange');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterPools: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate thread pools group.
            addProperty(res, cluster, 'publicThreadPoolSize');
            addProperty(res, cluster, 'systemThreadPoolSize');
            addProperty(res, cluster, 'managementThreadPoolSize');
            addProperty(res, cluster, 'igfsThreadPoolSize');
            res.needEmptyLine = true;

            return res;
        },
        xmlClusterTransactions: function(cluster, res) {
            if (!res)
                res = builder();

            // Generate transactions group.
            addBeanWithProperties(res, cluster.transactionConfiguration, 'transactionConfiguration',
                transactionConfiguration.className, transactionConfiguration.fields);
            res.needEmptyLine = true;

            return res;
        }
    }
}]);


// Filter to decode name using map(value, label).
controlCenterModule.filter('displayValue', function () {
    return function (v, m, dflt) {
        var i = _.findIndex(m, function (item) {
            return item.value == v;
        });

        if (i >= 0) {
            return m[i].label;
        }

        if (dflt) {
            return dflt;
        }

        return 'Unknown value';
    }
});

// Directive to enable validation for IP addresses.
controlCenterModule.directive('ipaddress', function () {
    const ip = '(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])';
    const port = '([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])';
    const portRange = '(:' + port + '(..' + port + ')?)?';
    const host = '(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])';

    return {
        require: 'ngModel',
        link: function (scope, elem, attrs, ctrl) {
            ctrl.$validators.ipaddress = function (modelValue, viewValue) {
                if (ctrl.$isEmpty(modelValue) || !attrs['ipaddress'])
                    return true;

                return viewValue.match(new RegExp('(^' + ip + portRange + '$)|(^' + host + portRange + '$)')) != null;
            }
        }
    }
});

// Directive to enable validation to match specified value.
controlCenterModule.directive('match', function ($parse) {
    return {
        require: 'ngModel',
        link: function (scope, elem, attrs, ctrl) {
            scope.$watch(function () {
                return $parse(attrs.match)(scope) === ctrl.$modelValue;
            }, function (currentValue) {
                ctrl.$setValidity('mismatch', currentValue);
            });
        }
    };
});

// Directive to bind ENTER key press with some user action.
controlCenterModule.directive('onEnter', function ($timeout) {
    return function (scope, elem, attrs) {
        elem.on('keydown keypress', function (event) {
            if (event.which === 13) {
                scope.$apply(function () {
                    $timeout(function () {
                        scope.$eval(attrs.onEnter)
                    });
                });

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('keydown keypress');
        });
    };
});

// Directive to bind ESC key press with some user action.
controlCenterModule.directive('onEscape', function () {
    return function (scope, elem, attrs) {
        elem.on('keydown keypress', function (event) {
            if (event.which === 27) {
                scope.$apply(function () {
                    scope.$eval(attrs.onEscape);
                });

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('keydown keypress');
        });
    };
});

// Directive to retain selection. To fix angular-strap typeahead bug with setting cursor to the end of text.
controlCenterModule.directive('retainSelection', function ($timeout) {
    return function (scope, elem, attr) {
        elem.on('keydown', function (evt) {
            var key = evt.which;
            var ctrlDown = evt.ctrlKey || evt.metaKey;
            var input = this;
            var start = input.selectionStart;

            $timeout(function () {
                var setCursor = false;

                // Handle Backspace[8].
                if (key == 8 && start > 0) {
                    start -= 1;

                    setCursor = true;
                }
                // Handle Del[46].
                else if (key == 46)
                    setCursor = true;
                // Handle: Caps Lock[20], Tab[9], Shift[16], Ctrl[17], Alt[18], Esc[27], Enter[13], Arrows[37..40], Home[36], End[35], Ins[45], PgUp[33], PgDown[34], F1..F12[111..124], Num Lock[], Scroll Lock[145].
                else if (!(key == 9 || key == 13 || (key > 15 && key < 20) || key == 27 ||
                    (key > 32 && key < 41) || key == 45 || (key > 111 && key < 124) || key == 144 || key == 145)) {
                    // Handle: Ctrl + [A[65], C[67], V[86]].
                    if (!(ctrlDown && (key = 65 || key == 67 || key == 86))) {
                        start += 1;

                        setCursor = true;
                    }
                }

                if (setCursor)
                    input.setSelectionRange(start, start);
            });
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('keydown');
        });
    };
});

// Factory function to focus element.
controlCenterModule.factory('$focus', function ($timeout) {
    return function (id) {
        // Timeout makes sure that is invoked after any other event has been triggered.
        // E.g. click events that need to run before the focus or inputs elements that are
        // in a disabled state but are enabled when those events are triggered.
        $timeout(function () {
            var elem = $('#' + id);

            if (elem.length > 0) {
                var offset = elem.offset();

                var elemOffset = offset.top;

                var winOffset = window.pageYOffset;

                if(elemOffset - 20 < winOffset || elemOffset + elem.outerHeight(true) + 20 > winOffset + window.innerHeight)
                    $('html, body').animate({
                        scrollTop: elemOffset - 20
                    }, 10);

                elem[0].focus();
            }
        });
    };
});

// Directive to focus next element on ENTER key.
controlCenterModule.directive('enterFocusNext', function ($focus) {
    return function (scope, elem, attrs) {
        elem.on('keydown keypress', function (event) {
            if (event.which === 13) {
                event.preventDefault();

                $focus(attrs.enterFocusNext);
            }
        });
    };
});

// Directive to mark elements to focus.
controlCenterModule.directive('onClickFocus', function ($focus) {
    return function (scope, elem, attr) {
        elem.on('click', function () {
            $focus(attr.onClickFocus);
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('click');
        });
    };
});

// Navigation bar controller.
controlCenterModule.controller('activeLink', [
    '$scope', function ($scope) {
        $scope.isActive = function (path) {
            return window.location.pathname.substr(0, path.length) == path;
        };
    }]);

// Login popup controller.
controlCenterModule.controller('auth', [
    '$scope', '$modal', '$http', '$window', '$common', '$focus',
    function ($scope, $modal, $http, $window, $common, $focus) {
        $scope.action = 'login';

        $scope.userDropdown = [{text: 'Profile', href: '/profile'}];

        if (!$scope.becomeUsed) {
            if ($scope.user && $scope.user.admin)
                $scope.userDropdown.push({text: 'Admin Panel', href: '/admin'});

            $scope.userDropdown.push({text: 'Log Out', href: '/logout'});
        }

        if ($scope.token && !$scope.error)
            $focus('user_password');

        // Pre-fetch modal dialogs.
        var loginModal = $modal({scope: $scope, templateUrl: '/login', show: false});

        // Show login modal.
        $scope.login = function () {
            loginModal.$promise.then(function () {
                loginModal.show();

                $focus('user_email');
            });
        };

        // Try to authorize user with provided credentials.
        $scope.auth = function (action, user_info) {
            $http.post('/' + action, user_info)
                .success(function () {
                    loginModal.hide();

                    $window.location = '/configuration/clusters';
                })
                .error(function (data, status) {
                    if (status == 403) {
                        loginModal.hide();

                        $window.location = '/password/reset';
                    }
                    else
                        $common.showError(data, 'top', '#errors-container');
                });
        };

        // Try to reset user password for provided token.
        $scope.resetPassword = function (reset_info) {
            $http.post('/password/reset', reset_info)
                .success(function (data) {
                    $scope.user_info = {email: data};
                    $scope.login();
                })
                .error(function (data, state) {
                    $common.showError(data);

                    if (state == 503) {
                        $scope.user_info = {};
                        $scope.login();
                    }
                });
        }
    }]);

// Download agent controller.
controlCenterModule.controller('agent-download', [
    '$scope', '$modal', function ($scope, $modal) {
        // Pre-fetch modal dialogs.
        var _agentDownloadModal = $modal({scope: $scope, templateUrl: '/agent/download', show: false});

        $scope.downloadAgent = function () {
            _agentDownloadModal.hide();

            var lnk = document.createElement('a');

            lnk.setAttribute('href', '/agent/ignite-web-agent-1.4.1-SNAPSHOT.zip');
            lnk.style.display = 'none';

            document.body.appendChild(lnk);

            lnk.click();

            document.body.removeChild(lnk);
        };

        $scope.showDownloadAgent = function () {
            _agentDownloadModal.$promise.then(_agentDownloadModal.show);
        };
}]);

// Navigation bar controller.
controlCenterModule.controller('notebooks', ['$scope', '$http', '$common', function ($scope, $http, $common) {
    $scope.$root.notebooks = [];

    $scope.$root.rebuildDropdown = function () {
        $scope.notebookDropdown = [
            {text: 'Create new notebook', href: '/notebooks/new', target: '_self'},
            {divider: true}
        ];

        _.forEach($scope.$root.notebooks, function (notebook) {
            $scope.notebookDropdown.push({
                text: notebook.name,
                href: '/sql/' + notebook._id,
                target: '_self'
            });
        });
    };

    $scope.$root.reloadNotebooks = function () {
        // When landing on the page, get clusters and show them.
        $http.post('/notebooks/list')
            .success(function (data) {
                $scope.$root.notebooks = data;

                $scope.$root.rebuildDropdown();
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.$root.reloadNotebooks();
}]);
