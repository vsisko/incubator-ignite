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

if (typeof window === 'undefined') {
    _ = require('lodash');

    $commonUtils = require('../../helpers/common-utils');
    $dataStructures = require('../../helpers/data-structures');
    $generatorCommon = require('./generator-common');
}

$generatorJava = {};

/**
 * Convert some name to valid java name.
 *
 * @param prefix To append to java name.
 * @param name to convert.
 * @returns {string} Valid java name.
 */
$generatorJava._toJavaName = function (prefix, name) {
    var javaName = name.replace(/[^A-Za-z_0-9]+/, '_');

    return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
};

$generatorJava._toJavaCode = function (val, type) {
    if (val == null)
        return 'null';

    if (type == 'float')
        return val + 'f';

    if (type == 'class')
        return val + '.class';

    if (type)
        return type + '.' + val;

    if (typeof(val) == 'string')
        return '"' + val.replace('"', '\\"') + '"';

    if (typeof(val) == 'number' || typeof(val) == 'boolean')
        return '' + val;

    throw "Unknown type: " + typeof(val) + ' (' + val + ')';
};

/**
 * Add eviction policy.
 *
 * @param res Resulting output with generated code.
 * @param varName Current using variable name.
 * @param evictionPolicy Data to add.
 * @param propertyName Name in source data.
 */
$generatorJava._addEvictionPolicy = function (res, varName, evictionPolicy, propertyName) {
    if (evictionPolicy && evictionPolicy.kind) {
        var e = $generatorCommon.EVICTION_POLICIES[evictionPolicy.kind];

        var obj = evictionPolicy[evictionPolicy.kind.toUpperCase()];

        $generatorJava._addBeanWithProperties(res, varName, obj, propertyName, propertyName, e.className, e.fields, true);
    }
};

$generatorJava._addCacheTypeMetadataDatabaseFields = function (res, meta, fieldProperty) {
    var dbFields = meta[fieldProperty];

    if (dbFields && dbFields.length > 0) {
        res.line();

        $generatorJava._declareVariable(res, $generatorJava._needNewVariable(res, fieldProperty), fieldProperty, 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.CacheTypeFieldMetadata');

        _.forEach(dbFields, function (field) {
            res.line(fieldProperty + '.add(new CacheTypeFieldMetadata(' +
                '"' + field.databaseName + '", ' +
                'java.sql.Types.' + field.databaseType + ', ' +
                '"' + field.javaName + '", ' +
                field.javaType + '.class'
                + '));');
        });

        res.line('typeMeta.' + $generatorJava._toJavaName('set', fieldProperty) + '(' + fieldProperty + ');');
    }
};

$generatorJava._addCacheTypeMetadataQueryFields = function (res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        res.line();

        $generatorJava._declareVariable(res, $generatorJava._needNewVariable(res, fieldProperty), fieldProperty, 'java.util.Map', 'java.util.LinkedHashMap', 'java.lang.String', 'java.lang.Class<?>');

        _.forEach(fields, function (field) {
            res.line(fieldProperty + '.put("' + field.name + '", ' + res.importClass(field.className) + '.class);');
        });

        res.line('typeMeta.' + $generatorJava._toJavaName('set', fieldProperty) + '(' + fieldProperty + ');');
    }
};

$generatorJava._addCacheTypeMetadataGroups = function (res, meta) {
    var groups = meta.groups;

    if (groups && groups.length > 0) {
        _.forEach(groups, function (group) {
            var fields = group.fields;

            if (fields && fields.length > 0) {
                res.importClass('java.util.Map');
                res.importClass('java.util.LinkedHashMap');
                res.importClass('org.apache.ignite.lang.IgniteBiTuple');

                var varNew = !res.groups;

                res.line();
                res.line((varNew ? 'Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> ' : '') +
                    "groups = new LinkedHashMap<>();");

                if (varNew)
                    res.groups = true;

                varNew = !res.groupItems;

                res.line((varNew ? 'LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> ' : '') +
                    'groupItems = new LinkedHashMap<>();');

                if (varNew)
                    res.groupItems = true;

                _.forEach(fields, function (field) {
                    res.line('groupItems.put("' + field.name + '", ' +
                        'new IgniteBiTuple<Class<?>, Boolean>(' + res.importClass(field.className) + '.class, ' + field.direction + '));');
                });

                res.line('groups.put("' + group.name + '", groupItems);');
            }
        });

        res.line('typeMeta.setGroups(groups);');
    }
};

$generatorJava._addCacheTypeMetadataConfiguration = function (res, meta) {
    $generatorJava._declareVariable(res, $generatorJava._needNewVariable(res, 'typeMeta'), 'typeMeta', 'org.apache.ignite.cache.CacheTypeMetadata');

    var kind = meta.kind;

    var keyType = $generatorJava._addClassProperty(res, 'typeMeta', meta, 'keyType');
    $generatorJava._addClassProperty(res, 'typeMeta', meta, 'valueType');

    if (kind != 'query') {
        $generatorJava._addProperty(res, 'typeMeta', meta, 'databaseSchema');
        $generatorJava._addProperty(res, 'typeMeta', meta, 'databaseTable');

        if (!$dataStructures.isJavaBuildInClass(keyType))
            $generatorJava._addCacheTypeMetadataDatabaseFields(res, meta, 'keyFields');

        $generatorJava._addCacheTypeMetadataDatabaseFields(res, meta, 'valueFields');
    }

    if (kind != 'store') {
        $generatorJava._addCacheTypeMetadataQueryFields(res, meta, 'queryFields');
        $generatorJava._addCacheTypeMetadataQueryFields(res, meta, 'ascendingFields');
        $generatorJava._addCacheTypeMetadataQueryFields(res, meta, 'descendingFields');

        res.needEmptyLine = true;
        $generatorJava._addListProperty(res, 'typeMeta', meta, 'textFields');

        $generatorJava._addCacheTypeMetadataGroups(res, meta);
    }

    res.line();
    res.line('types.add(typeMeta);');
    res.line();
};

$generatorJava._needNewVariable = function (res, varName) {
    var needNew = !res[varName];

    if (needNew)
        res[varName] = true;

    return needNew;
};

/**
 * Add variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varNew If 'true' then declare new variable otherwise reuse previously declared variable.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 * @param varFullActualType Variable actual full class name to be added to imports.
 * @param varFullGenericType1 Optional full class name of first generic.
 * @param varFullGenericType2 Optional full class name of second generic.
 */
$generatorJava._declareVariable = function (res, varNew, varName, varFullType, varFullActualType, varFullGenericType1, varFullGenericType2) {
    var varType = res.importClass(varFullType);

    if (varFullActualType && varFullGenericType1) {
        var varActualType = res.importClass(varFullActualType);
        var varGenericType1 = res.importClass(varFullGenericType1);

        if (varFullGenericType2)
            var varGenericType2 = res.importClass(varFullGenericType2);

        res.line((varNew ? (varType + '<' + varGenericType1 + (varGenericType2 ? ', ' + varGenericType2 : '') + '> ') : '') + varName + ' = new ' + varActualType + '<>();');
    }
    else
        res.line((varNew ? (varType + ' ') : '') + varName + ' = new ' + varType + '();');
};

/**
 * Add property via setter / property name.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 * @param enumType Optional info about property datatype.
 * @param setterName Optional special setter name.
 */
$generatorJava._addProperty = function (res, varName, obj, propName, enumType, setterName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val)) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava._setterName(setterName ? setterName : propName)
            + '(' + $generatorJava._toJavaCode(val, enumType) + ');');
    }

    return val;
};

/**
 * Add property via setter assuming that it is a 'Class'.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 */
$generatorJava._addClassProperty = function (res, varName, obj, propName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val)) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava._setterName(propName) + '(' + res.importClass(val) + '.class);');
    }
};

/**
 * @param propName Property name
 * @returns Property setter with name by java conventions.
 */
$generatorJava._setterName = function (propName) {
    return $generatorJava._toJavaName('set', propName);
};

$generatorJava._addListProperty = function (res, varName, obj, propName, enumType, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        res.importClass('java.util.Arrays');

        res.append(varName + '.' + $generatorJava._setterName(setterName ? setterName : propName) + '(Arrays.asList(');

        for (var i = 0; i < val.length; i++) {
            if (i > 0)
                res.append(', ');

            res.append($generatorJava._toJavaCode(val[i], enumType));
        }

        res.line('));');
    }
};

$generatorJava._addMultiparamProperty = function (res, varName, obj, propName, type, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.append(varName + '.' + $generatorJava._setterName(setterName ? setterName : propName) + '(');

        for (var i = 0; i < val.length; i++) {
            if (i > 0)
                res.append(', ');

            res.append($generatorJava._toJavaCode(val[i], type));
        }

        res.line(');');
    }
};

$generatorJava._addBeanWithProperties = function (res, varName, bean, beanPropName, beanVarName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && $commonUtils.hasProperty(bean, props)) {
        if (!res.emptyLineIfNeeded()) {
            res.line();
        }

        res.line(beanClass + ' ' + beanVarName + ' = new ' + beanClass + '();');

        for (var propName in props) {
            if (props.hasOwnProperty(propName)) {
                var descr = props[propName];

                if (descr) {
                    switch (descr.type) {
                        case 'list':
                            $generatorJava._addListProperty(res, beanVarName, bean, propName, descr.elementsType, descr.setterName);
                            break;

                        case 'enum':
                            $generatorJava._addProperty(res, beanVarName, bean, propName, descr.enumClass, descr.setterName);
                            break;

                        case 'float':
                            $generatorJava._addProperty(res, beanVarName, bean, propName, 'float', descr.setterName);
                            break;

                        case 'propertiesAsList':
                            var val = bean[propName];

                            if (val && val.length > 0) {
                                res.line('Properties ' + descr.propVarName + ' = new Properties();');

                                for (var i = 0; i < val.length; i++) {
                                    var nameAndValue = val[i];

                                    var eqIndex = nameAndValue.indexOf('=');
                                    if (eqIndex >= 0) {
                                        res.line(descr.propVarName + '.setProperty('
                                            + nameAndValue.substring(0, eqIndex) + ', '
                                            + nameAndValue.substr(eqIndex + 1) + ');');
                                    }

                                }

                                res.line(beanVarName + '.' + $generatorJava._setterName(propName) + '(' + descr.propVarName + ');');
                            }
                            break;

                        case 'className':
                            if (bean[propName]) {
                                res.line(beanVarName + '.' + $generatorJava._setterName(propName) + '(new ' + $generatorCommon.KNOWN_CLASSES[bean[propName]].className + '());');
                            }

                            break;

                        default:
                            $generatorJava._addProperty(res, beanVarName, bean, propName, null, descr.setterName);
                    }
                }
                else {
                    $generatorJava._addProperty(res, beanVarName, bean, propName);
                }
            }
        }

        res.line(varName + '.' + $generatorJava._setterName(beanPropName) + '(' + beanVarName + ');');

        res.needEmptyLine = true;
    }
    else if (createBeanAlthoughNoProps) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava._setterName(beanPropName) + '(new ' + beanClass + '());');
    }
};

// Generate cluster general group.
$generatorJava.clusterGeneral = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.discovery) {
        var d = cluster.discovery;

        $generatorJava._declareVariable(res, true, 'discovery', 'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi');

        switch (d.kind) {
            case 'Multicast':
                res.importClass('org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder');

                if (d.Multicast)
                    $generatorJava._addBeanWithProperties(res, 'discovery', d.Multicast, 'ipFinder', 'ipFinder',
                        'TcpDiscoveryMulticastIpFinder', {
                            multicastGroup: null,
                            multicastPort: null,
                            responseWaitTime: null,
                            addressRequestAttempts: null,
                            localAddress: null
                        }, true);
                else
                    res.line('discovery.setIpFinder(new TcpDiscoveryMulticastIpFinder());');


                break;

            case 'Vm':
                res.importClass('org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder');

                if (d.Vm)
                    $generatorJava._addBeanWithProperties(res, 'discovery', d.Vm, 'ipFinder', 'ipFinder',
                        'TcpDiscoveryVmIpFinder', {addresses: {type: 'list'}}, true);
                else
                    res.line('discovery.setIpFinder(new TcpDiscoveryVmIpFinder());');

                break;

            case 'S3':
                res.importClass('org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder');

                if (d.S3)
                    $generatorJava._addBeanWithProperties(res, 'discovery', d.S3, 'ipFinder', 'ipFinder',
                        'TcpDiscoveryS3IpFinder', {bucketName: null}, true);
                else
                    res.line('discovery.setIpFinder(new TcpDiscoveryS3IpFinder());');

                break;

            case 'Cloud':
                res.importClass('org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder');

                if (d.Cloud)
                    $generatorJava._addBeanWithProperties(res, 'discovery', d.Cloud, 'ipFinder', 'ipFinder',
                        'TcpDiscoveryCloudIpFinder', {
                            credential: null,
                            credentialPath: null,
                            identity: null,
                            provider: null,
                            regions: {type: 'list'},
                            zones: {type: 'list'}
                        }, true);
                else
                    res.line('discovery.setIpFinder(new TcpDiscoveryCloudIpFinder());');

                break;

            case 'GoogleStorage':
                res.importClass('org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder');

                if (d.GoogleStorage)
                    $generatorJava._addBeanWithProperties(res, 'discovery', d.GoogleStorage, 'ipFinder', 'ipFinder',
                        'TcpDiscoveryGoogleStorageIpFinder', {
                            projectName: null,
                            bucketName: null,
                            serviceAccountP12FilePath: null,
                            serviceAccountId: null
                        }, true);
                else
                    res.line('discovery.setIpFinder(new TcpDiscoveryGoogleStorageIpFinder());');

                break;

            case 'Jdbc':
                res.line();

                if (d.Jdbc) {
                    $generatorJava._declareVariable(res, true, 'ipFinder', 'org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder');

                    res.line('ipFinder.setInitSchema(' + ($commonUtils.isDefined(d.Jdbc.initSchema) && d.Jdbc.initSchema) + ');');
                    res.line('discovery.setIpFinder(ipFinder);');
                }
                else
                    res.line('discovery.setIpFinder(new TcpDiscoveryJdbcIpFinder());');

                break;

            case 'SharedFs':
                res.importClass('org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder');

                if (d.SharedFs)
                    $generatorJava._addBeanWithProperties(res, 'discovery', d.SharedFs, 'ipFinder', 'ipFinder',
                        'TcpDiscoverySharedFsIpFinder', {path: null}, true);
                else
                    res.line('discovery.setIpFinder(new TcpDiscoverySharedFsIpFinder());');

                break;

            default:
                throw 'Unknown discovery kind: ' + d.kind;
        }

        res.emptyLineIfNeeded();

        res.line('cfg.setDiscoverySpi(discovery);');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate atomics group.
$generatorJava.clusterAtomics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var atomicCfg = $generatorCommon.ATOMIC_CONFIGURATION;

    $generatorJava._addBeanWithProperties(res, 'cfg', cluster.atomicConfiguration, 'atomicConfiguration', 'atomicCfg',
        atomicCfg.className, atomicCfg.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate communication group.
$generatorJava.clusterCommunication = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, 'cfg', cluster, 'networkTimeout');
    $generatorJava._addProperty(res, 'cfg', cluster, 'networkSendRetryDelay');
    $generatorJava._addProperty(res, 'cfg', cluster, 'networkSendRetryCount');
    $generatorJava._addProperty(res, 'cfg', cluster, 'segmentCheckFrequency');
    $generatorJava._addProperty(res, 'cfg', cluster, 'waitForSegmentOnStart');
    $generatorJava._addProperty(res, 'cfg', cluster, 'discoveryStartupDelay');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, 'cfg', cluster, 'deploymentMode', 'DeploymentMode');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterEvents = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.includeEventTypes && cluster.includeEventTypes.length > 0) {
        res.emptyLineIfNeeded();

        if (cluster.includeEventTypes.length == 1) {
            res.importClass('org.apache.ignite.events.EventType');

            res.line('cfg.setIncludeEventTypes(EventType.' + cluster.includeEventTypes[0] + ');');
        }
        else {
            res.append('int[] events = new int[EventType.' + cluster.includeEventTypes[0] + '.length');

            for (i = 1; i < cluster.includeEventTypes.length; i++) {
                res.line();

                res.append('    + EventType.' + cluster.includeEventTypes[i] + '.length');
            }

            res.line('];');
            res.line();
            res.line('int k = 0;');

            for (i = 0; i < cluster.includeEventTypes.length; i++) {
                res.line();

                var e = cluster.includeEventTypes[i];

                res.line('System.arraycopy(EventType.' + e + ', 0, events, k, EventType.' + e + '.length);');
                res.line('k += EventType.' + e + '.length;');
            }

            res.line();
            res.line('cfg.setIncludeEventTypes(events);');
        }

        res.needEmptyLine = true;
    }

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterMarshaller = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        var marshallerDesc = $generatorCommon.MARSHALLERS[marshaller.kind];

        $generatorJava._addBeanWithProperties(res, 'cfg', marshaller[marshaller.kind], 'marshaller', 'marshaller',
            marshallerDesc.className, marshallerDesc.fields, true);

        $generatorJava._addBeanWithProperties(res, 'marshaller', marshaller[marshaller.kind], marshallerDesc.className, marshallerDesc.fields, true);
    }

    $generatorJava._addProperty(res, 'cfg', cluster, 'marshalLocalJobs');
    $generatorJava._addProperty(res, 'cfg', cluster, 'marshallerCacheKeepAliveTime');
    $generatorJava._addProperty(res, 'cfg', cluster, 'marshallerCacheThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterMetrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, 'cfg', cluster, 'metricsExpireTime');
    $generatorJava._addProperty(res, 'cfg', cluster, 'metricsHistorySize');
    $generatorJava._addProperty(res, 'cfg', cluster, 'metricsLogFrequency');
    $generatorJava._addProperty(res, 'cfg', cluster, 'metricsUpdateFrequency');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterP2p = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, 'cfg', cluster, 'peerClassLoadingEnabled');
    $generatorJava._addMultiparamProperty(res, 'cfg', cluster, 'peerClassLoadingLocalClassPathExclude');
    $generatorJava._addProperty(res, 'cfg', cluster, 'peerClassLoadingMissedResourcesCacheSize');
    $generatorJava._addProperty(res, 'cfg', cluster, 'peerClassLoadingThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterSwap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
        $generatorJava._addBeanWithProperties(res, 'cfg', cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi', 'swapSpi',
            $generatorCommon.SWAP_SPACE_SPI.className, $generatorCommon.SWAP_SPACE_SPI.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

$generatorJava.clusterTime = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, 'cfg', cluster, 'clockSyncSamples');
    $generatorJava._addProperty(res, 'cfg', cluster, 'clockSyncFrequency');
    $generatorJava._addProperty(res, 'cfg', cluster, 'timeServerPortBase');
    $generatorJava._addProperty(res, 'cfg', cluster, 'timeServerPortRange');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterPools = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, 'cfg', cluster, 'publicThreadPoolSize');
    $generatorJava._addProperty(res, 'cfg', cluster, 'systemThreadPoolSize');
    $generatorJava._addProperty(res, 'cfg', cluster, 'managementThreadPoolSize');
    $generatorJava._addProperty(res, 'cfg', cluster, 'igfsThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterTransactions = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addBeanWithProperties(res, 'cfg', cluster.transactionConfiguration, 'transactionConfiguration',
        'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION.className,
        $generatorCommon.TRANSACTION_CONFIGURATION.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate metadata general group.
$generatorJava.metadataGeneral = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.line('TODO');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for query group.
$generatorJava.metadataQuery = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.line('TODO');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for store group.
$generatorJava.metadataStore = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.line('TODO');

    res.needEmptyLine = true;

    return res;
};

/**
 * Generate java code for cache configuration.
 *
 * @param cache Cache config.
 * @param varName Variable name.
 * @param res Result builder.
 * @returns {*} Append generated java code to builder and return it.
 */
$generatorJava.cacheGeneral = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, varName, cache, 'name');

    res.importClass('org.apache.ignite.cache.CacheAtomicityMode');
    res.importClass('org.apache.ignite.cache.CacheMode');

    $generatorJava._addProperty(res, varName, cache, 'cacheMode', 'CacheMode');
    $generatorJava._addProperty(res, varName, cache, 'atomicityMode', 'CacheAtomicityMode');

    if (cache.cacheMode == 'PARTITIONED')
        $generatorJava._addProperty(res, varName, cache, 'backups');

    res.needEmptyLine = true;

    $generatorJava._addProperty(res, varName, cache, 'readFromBackup');
    $generatorJava._addProperty(res, varName, cache, 'copyOnRead');
    $generatorJava._addProperty(res, varName, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.cacheMemory = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, varName, cache, 'memoryMode', 'CacheMemoryMode');
    $generatorJava._addProperty(res, varName, cache, 'offHeapMaxMemory');

    res.needEmptyLine = true;

    $generatorJava._addEvictionPolicy(res, varName, cache.evictionPolicy, 'evictionPolicy');

    $generatorJava._addProperty(res, varName, cache, 'swapEnabled');
    $generatorJava._addProperty(res, varName, cache, 'startSize');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.cacheQuery = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, varName, cache, 'sqlOnheapRowCacheSize');
    $generatorJava._addProperty(res, varName, cache, 'longQueryWarningTimeout');

    if (cache.indexedTypes && cache.indexedTypes.length > 0) {
        res.emptyLineIfNeeded();

        res.append(varName + '.setIndexedTypes(');

        for (var i = 0; i < cache.indexedTypes.length; i++) {
            if (i > 0)
                res.append(', ');

            var pair = cache.indexedTypes[i];

            res.append($generatorJava._toJavaCode(res.importClass(pair.keyClass), 'class')).append(', ').append($generatorJava._toJavaCode(res.importClass(pair.valueClass), 'class'))
        }

        res.line(');');
    }

    $generatorJava._addMultiparamProperty(res, varName, cache, 'sqlFunctionClasses', 'class');

    $generatorJava._addProperty(res, varName, cache, 'sqlEscapeAll');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.cacheStore = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];
        var data = $generatorCommon.STORE_FACTORIES[cache.cacheStoreFactory.kind];

        var sfVarName = $generatorJava._toJavaName('storeFactory', cache.name);
        var dsVarName = 'none';

        if (storeFactory.dialect) {
            var dataSourceBean = storeFactory.dataSourceBean;

            dsVarName = $generatorJava._toJavaName('dataSource', dataSourceBean);

            if (!_.contains(res.datasources, dataSourceBean)) {
                res.datasources.push(dataSourceBean);

                var dataSource = $generatorCommon.DATA_SOURCES[storeFactory.dialect];

                res.line();

                $generatorJava._declareVariable(res, true, dsVarName, dataSource);

                res.line(dsVarName + '.setURL(_URL_);');
                res.line(dsVarName + '.setUsername(_User_Name_);');
                res.line(dsVarName + '.setPassword(_Password_);');
            }
        }

        $generatorJava._addBeanWithProperties(res, varName, storeFactory, 'cacheStoreFactory', sfVarName, data.className,
            data.fields, true);

        if (dsVarName != 'none')
            res.line(sfVarName + '.setDataSource(' + dsVarName + ');');

        res.needEmptyLine = true;
    }

    $generatorJava._addProperty(res, varName, cache, 'loadPreviousValue');
    $generatorJava._addProperty(res, varName, cache, 'readThrough');
    $generatorJava._addProperty(res, varName, cache, 'writeThrough');

    res.needEmptyLine = true;

    $generatorJava._addProperty(res, varName, cache, 'writeBehindEnabled');
    $generatorJava._addProperty(res, varName, cache, 'writeBehindBatchSize');
    $generatorJava._addProperty(res, varName, cache, 'writeBehindFlushSize');
    $generatorJava._addProperty(res, varName, cache, 'writeBehindFlushFrequency');
    $generatorJava._addProperty(res, varName, cache, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    return res;
};

// Generate cache type metadata configs.
$generatorJava.cacheMetadatas = function (qryMeta, storeMeta, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    // Generate cache type metadata configs.
    if ((qryMeta && qryMeta.length > 0) || (storeMeta && storeMeta.length > 0)) {
        res.emptyLineIfNeeded();

        $generatorJava._declareVariable(res, $generatorJava._needNewVariable(res, 'types'), 'types', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.CacheTypeMetadata');

        res.line();

        var metaNames = [];

        if (qryMeta && qryMeta.length > 0) {
            _.forEach(qryMeta, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    $generatorJava._addCacheTypeMetadataConfiguration(res, meta);
                }
            });
        }

        if (storeMeta && storeMeta.length > 0) {
            _.forEach(storeMeta, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    $generatorJava._addCacheTypeMetadataConfiguration(res, meta);
                }
            });
        }

        res.line(varName + '.setTypeMetadata(types);');

        res.needEmptyLine = true;
    }

    return res;
};

$generatorJava.cacheConcurrency = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, varName, cache, 'maxConcurrentAsyncOperations');
    $generatorJava._addProperty(res, varName, cache, 'defaultLockTimeout');
    $generatorJava._addProperty(res, varName, cache, 'atomicWriteOrderMode');

    res.needEmptyLine = true;

    return res;
};

$generatorJava.cacheRebalance = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode != 'LOCAL') {
        $generatorJava._addProperty(res, varName, cache, 'rebalanceMode', 'CacheRebalanceMode');
        $generatorJava._addProperty(res, varName, cache, 'rebalanceThreadPoolSize');
        $generatorJava._addProperty(res, varName, cache, 'rebalanceBatchSize');
        $generatorJava._addProperty(res, varName, cache, 'rebalanceOrder');
        $generatorJava._addProperty(res, varName, cache, 'rebalanceDelay');
        $generatorJava._addProperty(res, varName, cache, 'rebalanceTimeout');
        $generatorJava._addProperty(res, varName, cache, 'rebalanceThrottle');

        res.needEmptyLine = true;
    }

    return res;
};

$generatorJava.cacheServerNearCache = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode == 'PARTITIONED' && cache.nearCacheEnabled) {
        res.needEmptyLine = true;

        res.importClass('org.apache.ignite.configuration.NearCacheConfiguration');

        $generatorJava._addBeanWithProperties(res, varName, cache.nearConfiguration, 'nearConfiguration', 'nearConfiguration',
            'NearCacheConfiguration', {nearStartSize: null}, true);

        if (cache.nearConfiguration && cache.nearConfiguration.nearEvictionPolicy && cache.nearConfiguration.nearEvictionPolicy.kind) {
            $generatorJava._addEvictionPolicy(res, 'nearConfiguration', cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');
        }

        res.needEmptyLine = true;
    }

    return res;
};

$generatorJava.cacheStatistics = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava._addProperty(res, varName, cache, 'statisticsEnabled');
    $generatorJava._addProperty(res, varName, cache, 'managementEnabled');

    res.needEmptyLine = true;

    return res;
};

// Generate cache configs.
$generatorJava.cache = function(cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.cacheGeneral(cache, varName, res);

    $generatorJava.cacheMemory(cache, varName, res);

    $generatorJava.cacheQuery(cache, varName, res);

    $generatorJava.cacheStore(cache, varName, res);

    $generatorJava.cacheConcurrency(cache, varName, res);

    $generatorJava.cacheRebalance(cache, varName, res);

    $generatorJava.cacheServerNearCache(cache, varName, res);

    $generatorJava.cacheStatistics(cache, varName, res);

    $generatorJava.cacheMetadatas(cache.queryMetadata, cache.storeMetadata, varName, res);
};

// Generate cluster caches.
$generatorJava.clusterCaches = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var caches = cluster.caches;

    if (caches && caches.length > 0) {
        res.emptyLineIfNeeded();

        var names = [];

        _.forEach(caches, function (cache) {
            res.emptyLineIfNeeded();

            var cacheName = $generatorJava._toJavaName('cache', cache.name);

            $generatorJava._declareVariable(res, true, cacheName, 'org.apache.ignite.configuration.CacheConfiguration');

            $generatorJava.cache(cache, cacheName, res);

            names.push(cacheName);

            res.needEmptyLine = true;
        });

        res.emptyLineIfNeeded();

        res.append('cfg.setCacheConfiguration(');

        for (var i = 0; i < names.length; i++) {
            if (i > 0)
                res.append(', ');

            res.append(names[i]);
        }

        res.line(');');

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Function to generate java code for cluster configuration.
 *
 * @param cluster Cluster to process.
 * @param javaClass If 'true' then generate factory class otherwise generate code snippet.
 * @param clientNearConfiguration Near cache configuration for client node.
 */
$generatorJava.cluster = function (cluster, javaClass, clientNearConfiguration) {
    var res = $generatorCommon.builder();

    if (javaClass) {
        res.line('/**');
        res.line(' * ' + $generatorCommon.mainComment());
        res.line(' */');
        res.startBlock('public class ConfigurationFactory {');
        res.line('/**');
        res.line(' * Configure grid.');
        res.line(' */');
        res.startBlock('public IgniteConfiguration createConfiguration() {');
    }

    $generatorJava._declareVariable(res, true, 'cfg', 'org.apache.ignite.configuration.IgniteConfiguration');

    res.line();

    if (clientNearConfiguration) {
        res.line('cfg.setClientMode(true);');
        res.line();
    }

    $generatorJava.clusterGeneral(cluster, res);

    $generatorJava.clusterAtomics(cluster, res);

    $generatorJava.clusterCommunication(cluster, res);

    $generatorJava.clusterDeployment(cluster, res);

    $generatorJava.clusterEvents(cluster, res);

    $generatorJava.clusterMarshaller(cluster, res);

    $generatorJava.clusterMetrics(cluster, res);

    $generatorJava.clusterP2p(cluster, res);

    $generatorJava.clusterSwap(cluster, res);

    $generatorJava.clusterTime(cluster, res);

    $generatorJava.clusterPools(cluster, res);

    $generatorJava.clusterTransactions(cluster, res);

    if (javaClass) {
        res.line();
        res.line('return cfg;');
        res.endBlock('}');
        res.endBlock('}');

        return res.generateImports() + '\n\n' + res.join('')
    }

    return res.join('');
};

if (typeof window === 'undefined') {
    module.exports = $generatorJava;
}
