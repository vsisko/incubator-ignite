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

// For server side we should load required libraries.
if (typeof window === 'undefined') {
    _ = require('lodash');

    $commonUtils = require('../../helpers/common-utils');
    $dataStructures = require('../../helpers/data-structures');
    $generatorCommon = require('./generator-common');
}

// XML generation entry point.
$generatorJava = {};

/**
 * Convert some name to valid java name.
 *
 * @param prefix To append to java name.
 * @param name to convert.
 * @returns {string} Valid java name.
 */
$generatorJava.toJavaName = function (prefix, name) {
    var javaName = name.replace(/[^A-Za-z_0-9]+/, '_');

    return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
};

/**
 * Translate some value to valid java code.
 *
 * @param val Value to convert.
 * @param type Value type.
 * @returns {*} String with value that will be valid for java.
 */
$generatorJava.toJavaCode = function (val, type) {
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
 * @param propName Property name
 * @returns Property setter with name by java conventions.
 */
$generatorJava.setterName = function (propName) {
    return $generatorJava.toJavaName('set', propName);
};

/**
 * @param res Result holder.
 * @param varName Variable name to check.
 * @returns {boolean} 'true' if new variable required.
 */
$generatorJava.needNewVariable = function (res, varName) {
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
$generatorJava.declareVariable = function (res, varNew, varName, varFullType, varFullActualType, varFullGenericType1, varFullGenericType2) {
    res.emptyLineIfNeeded();

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

    res.needEmptyLine = true;
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
$generatorJava.property = function (res, varName, obj, propName, enumType, setterName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val)) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava.setterName(setterName ? setterName : propName)
            + '(' + $generatorJava.toJavaCode(val, enumType) + ');');
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
$generatorJava.classNameProperty = function (res, varName, obj, propName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val)) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava.setterName(propName) + '(' + res.importClass(val) + '.class);');
    }
};

// Add list property.
$generatorJava.listProperty = function (res, varName, obj, propName, enumType, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        res.importClass('java.util.Arrays');

        res.append(varName + '.' + $generatorJava.setterName(setterName ? setterName : propName) + '(Arrays.asList(');

        for (var i = 0; i < val.length; i++) {
            if (i > 0)
                res.append(', ');

            res.append($generatorJava.toJavaCode(val[i], enumType));
        }

        res.line('));');

        res.needEmptyLine = true;
    }
};

// Add multi-param property (setter with several arguments).
$generatorJava.multiparamProperty = function (res, varName, obj, propName, type, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        res.append(varName + '.' + $generatorJava.setterName(setterName ? setterName : propName) + '(');

        for (var i = 0; i < val.length; i++) {
            if (i > 0)
                res.append(', ');

            res.append($generatorJava.toJavaCode(val[i], type));
        }

        res.line(');');
    }
};

$generatorJava.beanProperty = function (res, varName, bean, beanPropName, beanVarName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && $commonUtils.hasProperty(bean, props)) {
        res.emptyLineIfNeeded();

        $generatorJava.declareVariable(res, true, beanVarName, beanClass);

        for (var propName in props) {
            if (props.hasOwnProperty(propName)) {
                var descr = props[propName];

                if (descr) {
                    switch (descr.type) {
                        case 'list':
                            $generatorJava.listProperty(res, beanVarName, bean, propName, descr.elementsType, descr.setterName);
                            break;

                        case 'enum':
                            $generatorJava.property(res, beanVarName, bean, propName, res.importClass(descr.enumClass), descr.setterName);
                            break;

                        case 'float':
                            $generatorJava.property(res, beanVarName, bean, propName, 'float', descr.setterName);
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

                                res.line(beanVarName + '.' + $generatorJava.setterName(propName) + '(' + descr.propVarName + ');');
                            }
                            break;

                        case 'jdbcDialect':
                            if (bean[propName]) {
                                var jdbcDialectClsName = res.importClass($generatorCommon.jdbcDialectClassName(bean[propName]));

                                res.line(beanVarName + '.' + $generatorJava.setterName(propName) + '(new ' + jdbcDialectClsName + '());');
                            }

                            break;

                        default:
                            $generatorJava.property(res, beanVarName, bean, propName, null, descr.setterName);
                    }
                }
                else {
                    $generatorJava.property(res, beanVarName, bean, propName);
                }
            }
        }

        res.needEmptyLine = true;

        res.line(varName + '.' + $generatorJava.setterName(beanPropName) + '(' + beanVarName + ');');

        res.needEmptyLine = true;
    }
    else if (createBeanAlthoughNoProps) {
        res.emptyLineIfNeeded();
        res.line(varName + '.' + $generatorJava.setterName(beanPropName) + '(new ' + res.importClass(beanClass) + '());');

        res.needEmptyLine = true;
    }
};

/**
 * Add eviction policy.
 *
 * @param res Resulting output with generated code.
 * @param varName Current using variable name.
 * @param evictionPolicy Data to add.
 * @param propertyName Name in source data.
 */
$generatorJava.evictionPolicy = function (res, varName, evictionPolicy, propertyName) {
    if (evictionPolicy && evictionPolicy.kind) {
        var evictionPolicyDesc = $generatorCommon.EVICTION_POLICIES[evictionPolicy.kind];

        var obj = evictionPolicy[evictionPolicy.kind.toUpperCase()];

        $generatorJava.beanProperty(res, varName, obj, propertyName, propertyName,
            evictionPolicyDesc.className, evictionPolicyDesc.fields, true);
    }
};

// Generate cluster general group.
$generatorJava.clusterGeneral = function (cluster, clientNearCfg, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.declareVariable(res, true, 'cfg', 'org.apache.ignite.configuration.IgniteConfiguration');

    if (clientNearCfg) {
        res.line('cfg.setClientMode(true);');

        res.needEmptyLine = true;
    }

    if (cluster.discovery) {
        var d = cluster.discovery;

        $generatorJava.declareVariable(res, true, 'discovery', 'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi');

        switch (d.kind) {
            case 'Multicast':
                $generatorJava.beanProperty(res, 'discovery', d.Multicast, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder',
                    {
                        multicastGroup: null,
                        multicastPort: null,
                        responseWaitTime: null,
                        addressRequestAttempts: null,
                        localAddress: null
                    }, true);

                break;

            case 'Vm':
                $generatorJava.beanProperty(res, 'discovery', d.Vm, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder',
                    {addresses: {type: 'list'}}, true);

                break;

            case 'S3':
                $generatorJava.beanProperty(res, 'discovery', d.S3, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder', {bucketName: null}, true);

                break;

            case 'Cloud':
                $generatorJava.beanProperty(res, 'discovery', d.Cloud, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder',
                    {
                        credential: null,
                        credentialPath: null,
                        identity: null,
                        provider: null,
                        regions: {type: 'list'},
                        zones: {type: 'list'}
                    }, true);

                break;

            case 'GoogleStorage':
                $generatorJava.beanProperty(res, 'discovery', d.GoogleStorage, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder',
                    {
                        projectName: null,
                        bucketName: null,
                        serviceAccountP12FilePath: null,
                        serviceAccountId: null
                    }, true);

                break;

            case 'Jdbc':
                $generatorJava.beanProperty(res, 'discovery', d.Jdbc, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder', {initSchema: null}, true);

                break;

            case 'SharedFs':
                $generatorJava.beanProperty(res, 'discovery', d.SharedFs, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder', {path: null}, true);

                break;

            default:
                res.line('Unknown discovery kind: ' + d.kind);
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

    $generatorJava.beanProperty(res, 'cfg', cluster.atomicConfiguration, 'atomicConfiguration', 'atomicCfg',
        atomicCfg.className, atomicCfg.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate communication group.
$generatorJava.clusterCommunication = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'networkTimeout');
    $generatorJava.property(res, 'cfg', cluster, 'networkSendRetryDelay');
    $generatorJava.property(res, 'cfg', cluster, 'networkSendRetryCount');
    $generatorJava.property(res, 'cfg', cluster, 'segmentCheckFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'waitForSegmentOnStart');
    $generatorJava.property(res, 'cfg', cluster, 'discoveryStartupDelay');

    res.needEmptyLine = true;

    return res;
};

// Generate deployment group.
$generatorJava.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'deploymentMode', 'DeploymentMode');

    res.needEmptyLine = true;

    return res;
};

// Generate events group.
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
                res.needEmptyLine = true;

                res.append('    + EventType.' + cluster.includeEventTypes[i] + '.length');
            }

            res.line('];');

            res.needEmptyLine = true;

            res.line('int k = 0;');

            for (i = 0; i < cluster.includeEventTypes.length; i++) {
                res.needEmptyLine = true;

                var e = cluster.includeEventTypes[i];

                res.line('System.arraycopy(EventType.' + e + ', 0, events, k, EventType.' + e + '.length);');
                res.line('k += EventType.' + e + '.length;');
            }

            res.needEmptyLine = true;

            res.line('cfg.setIncludeEventTypes(events);');
        }

        res.needEmptyLine = true;
    }

    res.needEmptyLine = true;

    return res;
};

// Generate marshaller group.
$generatorJava.clusterMarshaller = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        var marshallerDesc = $generatorCommon.MARSHALLERS[marshaller.kind];

        $generatorJava.beanProperty(res, 'cfg', marshaller[marshaller.kind], 'marshaller', 'marshaller',
            marshallerDesc.className, marshallerDesc.fields, true);

        $generatorJava.beanProperty(res, 'marshaller', marshaller[marshaller.kind], marshallerDesc.className, marshallerDesc.fields, true);
    }

    $generatorJava.property(res, 'cfg', cluster, 'marshalLocalJobs');
    $generatorJava.property(res, 'cfg', cluster, 'marshallerCacheKeepAliveTime');
    $generatorJava.property(res, 'cfg', cluster, 'marshallerCacheThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorJava.clusterMetrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'metricsExpireTime');
    $generatorJava.property(res, 'cfg', cluster, 'metricsHistorySize');
    $generatorJava.property(res, 'cfg', cluster, 'metricsLogFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'metricsUpdateFrequency');

    res.needEmptyLine = true;

    return res;
};

// Generate PeerClassLoading group.
$generatorJava.clusterP2p = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var p2pEnabled = cluster.peerClassLoadingEnabled;

    if ($commonUtils.isDefined(p2pEnabled)) {
        $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingEnabled');

        if (p2pEnabled) {
            $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingMissedResourcesCacheSize');
            $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingThreadPoolSize');
            $generatorJava.multiparamProperty(res, 'cfg', cluster, 'peerClassLoadingLocalClassPathExclude');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate swap group.
$generatorJava.clusterSwap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
        $generatorJava.beanProperty(res, 'cfg', cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi', 'swapSpi',
            $generatorCommon.SWAP_SPACE_SPI.className, $generatorCommon.SWAP_SPACE_SPI.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate time group.
$generatorJava.clusterTime = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'clockSyncSamples');
    $generatorJava.property(res, 'cfg', cluster, 'clockSyncFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'timeServerPortBase');
    $generatorJava.property(res, 'cfg', cluster, 'timeServerPortRange');

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorJava.clusterPools = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'publicThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'systemThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'managementThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'igfsThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate transactions group.
$generatorJava.clusterTransactions = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.beanProperty(res, 'cfg', cluster.transactionConfiguration, 'transactionConfiguration',
        'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION.className,
        $generatorCommon.TRANSACTION_CONFIGURATION.fields);

    return res;
};

// Generate cache general group.
$generatorJava.cacheGeneral = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'name');

    res.importClass('org.apache.ignite.cache.CacheAtomicityMode');
    res.importClass('org.apache.ignite.cache.CacheMode');

    $generatorJava.property(res, varName, cache, 'cacheMode', 'CacheMode');
    $generatorJava.property(res, varName, cache, 'atomicityMode', 'CacheAtomicityMode');

    if (cache.cacheMode == 'PARTITIONED')
        $generatorJava.property(res, varName, cache, 'backups');

    $generatorJava.property(res, varName, cache, 'readFromBackup');
    $generatorJava.property(res, varName, cache, 'copyOnRead');
    $generatorJava.property(res, varName, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

// Generate cache memory group.
$generatorJava.cacheMemory = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'memoryMode', 'CacheMemoryMode');
    $generatorJava.property(res, varName, cache, 'offHeapMaxMemory');

    res.needEmptyLine = true;

    $generatorJava.evictionPolicy(res, varName, cache.evictionPolicy, 'evictionPolicy');

    $generatorJava.property(res, varName, cache, 'swapEnabled');
    $generatorJava.property(res, varName, cache, 'startSize');

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorJava.cacheQuery = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'sqlOnheapRowCacheSize');
    $generatorJava.property(res, varName, cache, 'longQueryWarningTimeout');

    if (cache.indexedTypes && cache.indexedTypes.length > 0) {
        res.emptyLineIfNeeded();

        res.append(varName + '.setIndexedTypes(');

        for (var i = 0; i < cache.indexedTypes.length; i++) {
            if (i > 0)
                res.append(', ');

            var pair = cache.indexedTypes[i];

            res.append($generatorJava.toJavaCode(res.importClass(pair.keyClass), 'class')).append(', ').append($generatorJava.toJavaCode(res.importClass(pair.valueClass), 'class'))
        }

        res.line(');');
    }

    $generatorJava.multiparamProperty(res, varName, cache, 'sqlFunctionClasses', 'class');

    $generatorJava.property(res, varName, cache, 'sqlEscapeAll');

    res.needEmptyLine = true;

    return res;
};

// Generate cache store group.
$generatorJava.cacheStore = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

        if (storeFactory) {
            var storeFactoryDesc = $generatorCommon.STORE_FACTORIES[cache.cacheStoreFactory.kind];

            var sfVarName = $generatorJava.toJavaName('storeFactory', cache.name);
            var dsVarName = 'none';

            if (storeFactory.dialect) {
                var dataSourceBean = storeFactory.dataSourceBean;

                dsVarName = $generatorJava.toJavaName('dataSource', dataSourceBean);

                if (!_.contains(res.datasources, dataSourceBean)) {
                    res.datasources.push(dataSourceBean);

                    var dsClsName = $generatorCommon.dataSourceClassName(storeFactory.dialect);

                    res.needEmptyLine = true;

                    $generatorJava.declareVariable(res, true, dsVarName, dsClsName);

                    res.line(dsVarName + '.setURL(_URL_);');
                    res.line(dsVarName + '.setUsername(_User_Name_);');
                    res.line(dsVarName + '.setPassword(_Password_);');
                }
            }

            $generatorJava.beanProperty(res, varName, storeFactory, 'cacheStoreFactory', sfVarName,
                storeFactoryDesc.className, storeFactoryDesc.fields, true);

            if (dsVarName != 'none')
                res.line(sfVarName + '.setDataSource(' + dsVarName + ');');

            res.needEmptyLine = true;
        }
    }

    $generatorJava.property(res, varName, cache, 'loadPreviousValue');
    $generatorJava.property(res, varName, cache, 'readThrough');
    $generatorJava.property(res, varName, cache, 'writeThrough');

    res.needEmptyLine = true;

    $generatorJava.property(res, varName, cache, 'writeBehindEnabled');
    $generatorJava.property(res, varName, cache, 'writeBehindBatchSize');
    $generatorJava.property(res, varName, cache, 'writeBehindFlushSize');
    $generatorJava.property(res, varName, cache, 'writeBehindFlushFrequency');
    $generatorJava.property(res, varName, cache, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    return res;
};

// Generate cache concurrency group.
$generatorJava.cacheConcurrency = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'maxConcurrentAsyncOperations');
    $generatorJava.property(res, varName, cache, 'defaultLockTimeout');
    $generatorJava.property(res, varName, cache, 'atomicWriteOrderMode');

    res.needEmptyLine = true;

    return res;
};

// Generate cache rebalance group.
$generatorJava.cacheRebalance = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode != 'LOCAL') {
        $generatorJava.property(res, varName, cache, 'rebalanceMode', 'CacheRebalanceMode');
        $generatorJava.property(res, varName, cache, 'rebalanceThreadPoolSize');
        $generatorJava.property(res, varName, cache, 'rebalanceBatchSize');
        $generatorJava.property(res, varName, cache, 'rebalanceOrder');
        $generatorJava.property(res, varName, cache, 'rebalanceDelay');
        $generatorJava.property(res, varName, cache, 'rebalanceTimeout');
        $generatorJava.property(res, varName, cache, 'rebalanceThrottle');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache server near cache group.
$generatorJava.cacheServerNearCache = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode == 'PARTITIONED' && cache.nearCacheEnabled) {
        res.needEmptyLine = true;

        res.importClass('org.apache.ignite.configuration.NearCacheConfiguration');

        $generatorJava.beanProperty(res, varName, cache.nearConfiguration, 'nearConfiguration', 'nearConfiguration',
            'NearCacheConfiguration', {nearStartSize: null}, true);

        if (cache.nearConfiguration && cache.nearConfiguration.nearEvictionPolicy && cache.nearConfiguration.nearEvictionPolicy.kind) {
            $generatorJava.evictionPolicy(res, 'nearConfiguration', cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache statistics group.
$generatorJava.cacheStatistics = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'statisticsEnabled');
    $generatorJava.property(res, varName, cache, 'managementEnabled');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata query fields.
$generatorJava.metadataQueryFields = function (res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, fieldProperty), fieldProperty, 'java.util.Map', 'java.util.LinkedHashMap', 'java.lang.String', 'java.lang.Class<?>');

        _.forEach(fields, function (field) {
            res.line(fieldProperty + '.put("' + field.name + '", ' + res.importClass(field.className) + '.class);');
        });

        res.needEmptyLine = true;

        res.line('typeMeta.' + $generatorJava.toJavaName('set', fieldProperty) + '(' + fieldProperty + ');');

        res.needEmptyLine = true;
    }
};

// Generate metadata groups.
$generatorJava.metadataGroups = function (res, meta) {
    var groups = meta.groups;

    if (groups && groups.length > 0) {
        _.forEach(groups, function (group) {
            var fields = group.fields;

            if (fields && fields.length > 0) {
                res.importClass('java.util.Map');
                res.importClass('java.util.LinkedHashMap');
                res.importClass('org.apache.ignite.lang.IgniteBiTuple');

                var varNew = !res.groups;

                res.needEmptyLine = true;

                res.line((varNew ? 'Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> ' : '') +
                    "groups = new LinkedHashMap<>();");

                res.needEmptyLine = true;

                if (varNew)
                    res.groups = true;

                varNew = !res.groupItems;

                res.line((varNew ? 'LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> ' : '') +
                    'groupItems = new LinkedHashMap<>();');

                res.needEmptyLine = true;

                if (varNew)
                    res.groupItems = true;

                _.forEach(fields, function (field) {
                    res.line('groupItems.put("' + field.name + '", ' +
                        'new IgniteBiTuple<Class<?>, Boolean>(' + res.importClass(field.className) + '.class, ' + field.direction + '));');
                });

                res.needEmptyLine = true;

                res.line('groups.put("' + group.name + '", groupItems);');
            }
        });

        res.needEmptyLine = true;

        res.line('typeMeta.setGroups(groups);');

        res.needEmptyLine = true;
    }
};

// Generate metadata db fields.
$generatorJava.metadataDatabaseFields = function (res, meta, fieldProperty) {
    var dbFields = meta[fieldProperty];

    if (dbFields && dbFields.length > 0) {
        res.needEmptyLine = true;

        $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, fieldProperty), fieldProperty, 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.CacheTypeFieldMetadata');

        _.forEach(dbFields, function (field) {
            res.line(fieldProperty + '.add(new CacheTypeFieldMetadata(' +
                '"' + field.databaseName + '", ' +
                'java.sql.Types.' + field.databaseType + ', ' +
                '"' + field.javaName + '", ' +
                field.javaType + '.class'
                + '));');
        });

        res.line('typeMeta.' + $generatorJava.toJavaName('set', fieldProperty) + '(' + fieldProperty + ');');

        res.needEmptyLine = true;
    }
};

// Generate metadata general group.
$generatorJava.metadataGeneral = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.classNameProperty(res, 'typeMeta', meta, 'keyType');
    $generatorJava.classNameProperty(res, 'typeMeta', meta, 'valueType');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for query group.
$generatorJava.metadataQuery = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.metadataQueryFields(res, meta, 'queryFields');
    $generatorJava.metadataQueryFields(res, meta, 'ascendingFields');
    $generatorJava.metadataQueryFields(res, meta, 'descendingFields');

    $generatorJava.listProperty(res, 'typeMeta', meta, 'textFields');

    $generatorJava.metadataGroups(res, meta);

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for store group.
$generatorJava.metadataStore = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'typeMeta', meta, 'databaseSchema');
    $generatorJava.property(res, 'typeMeta', meta, 'databaseTable');

    if (!$dataStructures.isJavaBuildInClass(meta.keyType))
        $generatorJava.metadataDatabaseFields(res, meta, 'keyFields');

    $generatorJava.metadataDatabaseFields(res, meta, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

// Generate cache type metadata config.
$generatorJava.cacheMetadata = function(meta, res) {
    $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, 'typeMeta'), 'typeMeta', 'org.apache.ignite.cache.CacheTypeMetadata');

    $generatorJava.metadataGeneral(meta, res);
    $generatorJava.metadataQuery(meta, res);
    $generatorJava.metadataStore(meta, res);

    res.emptyLineIfNeeded();
    res.line('types.add(typeMeta);');

    res.needEmptyLine = true;
};

// Generate cache type metadata configs.
$generatorJava.cacheMetadatas = function (qryMeta, storeMeta, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    // Generate cache type metadata configs.
    if ((qryMeta && qryMeta.length > 0) || (storeMeta && storeMeta.length > 0)) {
        $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, 'types'), 'types', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.CacheTypeMetadata');

        var metaNames = [];

        if (qryMeta && qryMeta.length > 0) {
            _.forEach(qryMeta, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    $generatorJava.cacheMetadata(meta, res);
                }
            });
        }

        if (storeMeta && storeMeta.length > 0) {
            _.forEach(storeMeta, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    $generatorJava.cacheMetadata(meta, res);
                }
            });
        }

        res.line(varName + '.setTypeMetadata(types);');

        res.needEmptyLine = true;
    }

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
$generatorJava.clusterCaches = function (caches, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (caches && caches.length > 0) {
        res.emptyLineIfNeeded();

        var names = [];

        _.forEach(caches, function (cache) {
            res.emptyLineIfNeeded();

            var cacheName = $generatorJava.toJavaName('cache', cache.name);

            $generatorJava.declareVariable(res, true, cacheName, 'org.apache.ignite.configuration.CacheConfiguration');

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
 * @param clientNearCfg Near cache configuration for client node.
 */
$generatorJava.cluster = function (cluster, javaClass, clientNearCfg) {
    var res = $generatorCommon.builder();

    if (cluster) {
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

        $generatorJava.clusterGeneral(cluster, clientNearCfg, res);

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

        $generatorJava.clusterCaches(cluster.caches, res);

        if (javaClass) {
            res.needEmptyLine = true;

            res.line('return cfg;');
            res.endBlock('}');
            res.endBlock('}');

            return res.generateImports() + '\n\n' + res.join('')
        }
    }

    return res.join('');
};

// For server side we should export Java code generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorJava;
}
