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
$generatorXml = {};

// Do XML escape.
$generatorXml.escape = function (s) {
    if (typeof(s) != 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
};

// Add XML element.
$generatorXml.element = function (res, tag, attr1, val1, attr2, val2) {
    var elem = '<' + tag;

    if (attr1)
        elem += ' ' + attr1 + '="' + val1 + '"';

    if (attr2)
        elem += ' ' + attr2 + '="' + val2 + '"';

    elem += '/>';

    res.emptyLineIfNeeded();
    res.line(elem);
};

// Add property.
$generatorXml.property = function (res, obj, propName, setterName) {
    if ($commonUtils.isDefined(obj)) {
        var val = obj[propName];

        if ($commonUtils.isDefined(val))
            $generatorXml.element(res, 'property', 'name', setterName ? setterName : propName, 'value', $generatorXml.escape(val));
    }
};

// Add property for class name.
$generatorXml.classNameProperty = function (res, obj, propName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val))
        $generatorXml.element(res, 'property', 'name', propName, 'value', $dataStructures.fullClassName(val));
};

// Add list property.
$generatorXml.listProperty = function (res, obj, propName, listType, rowFactory) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        if (!listType)
            listType = 'list';

        if (!rowFactory)
            rowFactory = function (val) {
                return '<value>' + $generatorXml.escape(val) + '</value>'
            };

        res.startBlock('<property name="' + propName + '">');
        res.startBlock('<' + listType + '>');

        for (var i = 0; i < val.length; i++)
            res.line(rowFactory(val[i]));

        res.endBlock('</' + listType + '>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Add bean property.
$generatorXml.beanProperty = function (res, bean, beanPropName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && $commonUtils.hasProperty(bean, props)) {
        res.emptyLineIfNeeded();
        res.startBlock('<property name="' + beanPropName + '">');
        res.startBlock('<bean class="' + beanClass + '">');

        for (var propName in props) {
            if (props.hasOwnProperty(propName)) {
                var descr = props[propName];

                if (descr) {
                    if (descr.type == 'list') {
                        $generatorXml.listProperty(res, bean, propName, descr.setterName);
                    }
                    else if (descr.type == 'jdbcDialect') {
                        if (bean[propName]) {
                            res.startBlock('<property name="' + propName + '">');
                            res.line('<bean class="' + $generatorCommon.jdbcDialectClassName(bean[propName]) + '"/>');
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
                                    res.line('<prop key="' + $generatorXml.escape(nameAndValue.substring(0, eqIndex)) + '">' +
                                        $generatorXml.escape(nameAndValue.substr(eqIndex + 1)) + '</prop>');
                                }
                            }

                            res.endBlock('</props>');
                            res.endBlock('</property>');
                        }
                    }
                    else
                        $generatorXml.property(res, bean, propName, descr.setterName);
                }
                else
                    $generatorXml.property(res, bean, propName);
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
};

// Generate eviction policy.
$generatorXml.evictionPolicy = function (res, evtPlc, propName) {
    if (evtPlc && evtPlc.kind) {
        var e = $generatorCommon.EVICTION_POLICIES[evtPlc.kind];

        var obj = evtPlc[evtPlc.kind.toUpperCase()];

        $generatorXml.beanProperty(res, obj, propName, e.className, e.fields, true);
    }
};

// Generate discovery.
$generatorXml.clusterGeneral = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.discovery) {
        res.startBlock('<property name="discoverySpi">');
        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">');
        res.startBlock('<property name="ipFinder">');

        var d = cluster.discovery;

        switch (d.kind) {
            case 'Multicast':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder">');

                if (d.Multicast) {
                    $generatorXml.property(res, d.Multicast, 'multicastGroup');
                    $generatorXml.property(res, d.Multicast, 'multicastPort');
                    $generatorXml.property(res, d.Multicast, 'responseWaitTime');
                    $generatorXml.property(res, d.Multicast, 'addressRequestAttempts');
                    $generatorXml.property(res, d.Multicast, 'localAddress');
                }

                res.endBlock('</bean>');

                break;

            case 'Vm':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">');

                if (d.Vm) {
                    $generatorXml.listProperty(res, d.Vm, 'addresses');
                }

                res.endBlock('</bean>');

                break;

            case 'S3':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder">');

                if (d.S3) {
                    if (d.S3.bucketName)
                        res.line('<property name="bucketName" value="' + $generatorXml.escape(d.S3.bucketName) + '" />');
                }

                res.endBlock('</bean>');

                break;

            case 'Cloud':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder">');

                if (d.Cloud) {
                    $generatorXml.property(res, d.Cloud, 'credential');
                    $generatorXml.property(res, d.Cloud, 'credentialPath');
                    $generatorXml.property(res, d.Cloud, 'identity');
                    $generatorXml.property(res, d.Cloud, 'provider');
                    $generatorXml.listProperty(res, d.Cloud, 'regions');
                    $generatorXml.listProperty(res, d.Cloud, 'zones');
                }

                res.endBlock('</bean>');

                break;

            case 'GoogleStorage':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder">');

                if (d.GoogleStorage) {
                    $generatorXml.property(res, d.GoogleStorage, 'projectName');
                    $generatorXml.property(res, d.GoogleStorage, 'bucketName');
                    $generatorXml.property(res, d.GoogleStorage, 'serviceAccountP12FilePath');
                    $generatorXml.property(res, d.GoogleStorage, 'serviceAccountId');
                }

                res.endBlock('</bean>');

                break;

            case 'Jdbc':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder">');

                if (d.Jdbc) {
                    res.line('<property name="initSchema" value="' + ($commonUtils.isDefined(d.Jdbc.initSchema) && d.Jdbc.initSchema) + '"/>');
                }

                res.endBlock('</bean>');

                break;

            case 'SharedFs':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder">');

                if (d.SharedFs) {
                    $generatorXml.property(res, d.SharedFs, 'path');
                }

                res.endBlock('</bean>');

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
};

// Generate atomics group.
$generatorXml.clusterAtomics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var atomicCfg = $generatorCommon.ATOMIC_CONFIGURATION;

    $generatorXml.beanProperty(res, cluster.atomicConfiguration, 'atomicConfiguration', atomicCfg.className, atomicCfg.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate communication group.
$generatorXml.clusterCommunication = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'networkTimeout');
    $generatorXml.property(res, cluster, 'networkSendRetryDelay');
    $generatorXml.property(res, cluster, 'networkSendRetryCount');
    $generatorXml.property(res, cluster, 'segmentCheckFrequency');
    $generatorXml.property(res, cluster, 'waitForSegmentOnStart');
    $generatorXml.property(res, cluster, 'discoveryStartupDelay');

    res.needEmptyLine = true;

    return res;
};

// Generate deployment group.
$generatorXml.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'deploymentMode');

    res.needEmptyLine = true;

    return res;
};

// Generate events group.
$generatorXml.clusterEvents = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

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

                var eventList = $dataStructures.EVENT_GROUPS[eventGroup];

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
};

// Generate marshaller group.
$generatorXml.clusterMarshaller = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        var marshallerDesc = $generatorCommon.MARSHALLERS[marshaller.kind];

        $generatorXml.beanProperty(res, marshaller[marshaller.kind], 'marshaller', marshallerDesc.className, marshallerDesc.fields, true);

        res.needEmptyLine = true;
    }

    $generatorXml.property(res, cluster, 'marshalLocalJobs');
    $generatorXml.property(res, cluster, 'marshallerCacheKeepAliveTime');
    $generatorXml.property(res, cluster, 'marshallerCacheThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorXml.clusterMetrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'metricsExpireTime');
    $generatorXml.property(res, cluster, 'metricsHistorySize');
    $generatorXml.property(res, cluster, 'metricsLogFrequency');
    $generatorXml.property(res, cluster, 'metricsUpdateFrequency');

    res.needEmptyLine = true;

    return res;
};

// Generate PeerClassLoading group.
$generatorXml.clusterP2p = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var p2pEnabled = cluster.peerClassLoadingEnabled;

    if ($commonUtils.isDefined(p2pEnabled)) {
        $generatorXml.property(res, cluster, 'peerClassLoadingEnabled');

        if (p2pEnabled) {
            $generatorXml.property(res, cluster, 'peerClassLoadingMissedResourcesCacheSize');
            $generatorXml.property(res, cluster, 'peerClassLoadingThreadPoolSize');
            $generatorXml.listProperty(res, cluster, 'peerClassLoadingLocalClassPathExclude');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate swap group.
$generatorXml.clusterSwap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var swapSpaceSpi = $generatorCommon.SWAP_SPACE_SPI;

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
        $generatorXml.beanProperty(res, cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi',
            swapSpaceSpi.className, swapSpaceSpi.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate time group.
$generatorXml.clusterTime = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'clockSyncSamples');
    $generatorXml.property(res, cluster, 'clockSyncFrequency');
    $generatorXml.property(res, cluster, 'timeServerPortBase');
    $generatorXml.property(res, cluster, 'timeServerPortRange');

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorXml.clusterPools = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'publicThreadPoolSize');
    $generatorXml.property(res, cluster, 'systemThreadPoolSize');
    $generatorXml.property(res, cluster, 'managementThreadPoolSize');
    $generatorXml.property(res, cluster, 'igfsThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate transactions group.
$generatorXml.clusterTransactions = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var trnCfg = $generatorCommon.TRANSACTION_CONFIGURATION;

    $generatorXml.beanProperty(res, cluster.transactionConfiguration, 'transactionConfiguration', trnCfg.className, trnCfg.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate cache general group.
$generatorXml.cacheGeneral = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'name');

    $generatorXml.property(res, cache, 'cacheMode');
    $generatorXml.property(res, cache, 'atomicityMode');

    if (cache.cacheMode == 'PARTITIONED')
        $generatorXml.property(res, cache, 'backups');

    $generatorXml.property(res, cache, 'readFromBackup');
    $generatorXml.property(res, cache, 'copyOnRead');
    $generatorXml.property(res, cache, 'invalidate');

    return res;
};

// Generate cache memory group.
$generatorXml.cacheMemory = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'memoryMode');
    $generatorXml.property(res, cache, 'offHeapMaxMemory');

    res.needEmptyLine = true;

    $generatorXml.evictionPolicy(res, cache.evictionPolicy, 'evictionPolicy');

    res.needEmptyLine = true;

    $generatorXml.property(res, cache, 'swapEnabled');
    $generatorXml.property(res, cache, 'startSize');

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorXml.cacheQuery = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'sqlOnheapRowCacheSize');
    $generatorXml.property(res, cache, 'longQueryWarningTimeout');

    if (cache.indexedTypes && cache.indexedTypes.length > 0) {
        res.startBlock('<property name="indexedTypes">');
        res.startBlock('<list>');

        for (var i = 0; i < cache.indexedTypes.length; i++) {
            var pair = cache.indexedTypes[i];

            res.line('<value>' + $dataStructures.fullClassName(pair.keyClass) + '</value>');
            res.line('<value>' + $dataStructures.fullClassName(pair.valueClass) + '</value>');
        }

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    $generatorXml.listProperty(res, cache, 'sqlFunctionClasses', 'array');

    $generatorXml.property(res, cache, 'sqlEscapeAll');

    res.needEmptyLine = true;

    return res;
};

// Generate cache store group.
$generatorXml.cacheStore = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

        if (storeFactory) {
            var storeFactoryDesc = $generatorCommon.STORE_FACTORIES[cache.cacheStoreFactory.kind];

            $generatorXml.beanProperty(res, storeFactory, 'cacheStoreFactory', storeFactoryDesc.className, storeFactoryDesc.fields, true);

            if (storeFactory.dialect) {
                if (_.findIndex(res.datasources, function (ds) {
                        return ds.dataSourceBean == storeFactory.dataSourceBean;
                    }) < 0) {
                    res.datasources.push({
                        dataSourceBean: storeFactory.dataSourceBean,
                        className: $generatorCommon.DATA_SOURCES[storeFactory.dialect]
                    });
                }
            }

            res.needEmptyLine = true;
        }
    }

    $generatorXml.property(res, cache, 'loadPreviousValue');
    $generatorXml.property(res, cache, 'readThrough');
    $generatorXml.property(res, cache, 'writeThrough');

    res.needEmptyLine = true;

    $generatorXml.property(res, cache, 'writeBehindEnabled');
    $generatorXml.property(res, cache, 'writeBehindBatchSize');
    $generatorXml.property(res, cache, 'writeBehindFlushSize');
    $generatorXml.property(res, cache, 'writeBehindFlushFrequency');
    $generatorXml.property(res, cache, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    return res;
};

// Generate cache concurrency group.
$generatorXml.cacheConcurrency = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'maxConcurrentAsyncOperations');
    $generatorXml.property(res, cache, 'defaultLockTimeout');
    $generatorXml.property(res, cache, 'atomicWriteOrderMode');

    res.needEmptyLine = true;

    return res;
};

// Generate cache rebalance group.
$generatorXml.cacheRebalance = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode != 'LOCAL') {
        $generatorXml.property(res, cache, 'rebalanceMode');
        $generatorXml.property(res, cache, 'rebalanceThreadPoolSize');
        $generatorXml.property(res, cache, 'rebalanceBatchSize');
        $generatorXml.property(res, cache, 'rebalanceOrder');
        $generatorXml.property(res, cache, 'rebalanceDelay');
        $generatorXml.property(res, cache, 'rebalanceTimeout');
        $generatorXml.property(res, cache, 'rebalanceThrottle');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache server near cache group.
$generatorXml.cacheServerNearCache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.nearCacheEnabled) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="nearConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (cache.nearConfiguration && cache.nearConfiguration.nearStartSize)
            $generatorXml.property(res, cache.nearConfiguration, 'nearStartSize');

        if (cache.nearConfiguration && cache.nearConfiguration.nearEvictionPolicy.kind)
            $generatorXml.evictionPolicy(res, cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');

        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    res.needEmptyLine = true;

    return res;
};

// Generate cache statistics group.
$generatorXml.cacheStatistics = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'statisticsEnabled');
    $generatorXml.property(res, cache, 'managementEnabled');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata query fields.
$generatorXml.metadataQueryFields = function (res, meta, fieldProp) {
    var fields = meta[fieldProp];

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="' + fieldProp + '">');
        res.startBlock('<map>');

        _.forEach(fields, function (field) {
            $generatorXml.element(res, 'entry', 'key', field.name, 'value', $dataStructures.fullClassName(field.className));
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate metadata groups.
$generatorXml.metadataGroups = function (res, meta) {
    var groups = meta.groups;

    if (groups && groups.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="groups">');
        res.startBlock('<map>');

        _.forEach(groups, function (group) {
            var fields = group.fields;

            if (fields && fields.length > 0) {
                res.startBlock('<entry key="' + group.name + '">');
                res.startBlock('<map>');

                _.forEach(fields, function (field) {
                    res.startBlock('<entry key="' + field.name + '">');

                    res.startBlock('<bean class="org.apache.ignite.lang.IgniteBiTuple">');
                    res.line('<constructor-arg value="' + $dataStructures.fullClassName(field.className) + '"/>');
                    res.line('<constructor-arg value="' + field.direction + '"/>');
                    res.endBlock('</bean>');

                    res.endBlock('</entry>');
                });

                res.endBlock('</map>');
                res.endBlock('</entry>');
            }
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate metadata db fields.
$generatorXml.metadataDatabaseFields = function (res, meta, fieldProp) {
    var fields = meta[fieldProp];

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="' + fieldProp + '">');

        res.startBlock('<list>');

        _.forEach(fields, function (field) {
            res.startBlock('<bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">');

            $generatorXml.property(res, field, 'databaseName');

            res.startBlock('<property name="databaseType">');
            res.line('<util:constant static-field="java.sql.Types.' + field.databaseType + '"/>');
            res.endBlock('</property>');

            $generatorXml.property(res, field, 'javaName');

            $generatorXml.classNameProperty(res, field, 'javaType');

            res.endBlock('</bean>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate metadata general group.
$generatorXml.metadataGeneral = function(meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.classNameProperty(res, meta, 'keyType');
    $generatorXml.property(res, meta, 'valueType');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for query group.
$generatorXml.metadataQuery = function(meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.metadataQueryFields(res, meta, 'queryFields');
    $generatorXml.metadataQueryFields(res, meta, 'ascendingFields');
    $generatorXml.metadataQueryFields(res, meta, 'descendingFields');

    $generatorXml.listProperty(res, meta, 'textFields');

    $generatorXml.metadataGroups(res, meta);

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for store group.
$generatorXml.metadataStore = function(meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, meta, 'databaseSchema');
    $generatorXml.property(res, meta, 'databaseTable');

    if (!$dataStructures.isJavaBuildInClass(meta.keyType))
        $generatorXml.metadataDatabaseFields(res, meta, 'keyFields');

    $generatorXml.metadataDatabaseFields(res, meta, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

// Generate cache type metadata config.
$generatorXml.cacheMetadata = function(meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.cache.CacheTypeMetadata">');

    $generatorXml.metadataGeneral(meta, res);
    $generatorXml.metadataQuery(meta, res);
    $generatorXml.metadataStore(meta, res);

    res.endBlock('</bean>');

    res.needEmptyLine = true;

    return res;
};

// Generate cache type metadata configs.
$generatorXml.cacheMetadatas = function(qryMeta, storeMeta, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ((qryMeta && qryMeta.length > 0) ||
        (storeMeta && storeMeta.length > 0)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="typeMetadata">');
        res.startBlock('<list>');

        var metaNames = [];

        if (qryMeta && qryMeta.length > 0) {
            _.forEach(qryMeta, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    $generatorXml.cacheMetadata(meta, res);
                }
            });
        }

        if (storeMeta && storeMeta.length > 0) {
            _.forEach(storeMeta, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    $generatorXml.cacheMetadata(meta, res);
                }
            });
        }

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache configs.
$generatorXml.cache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.configuration.CacheConfiguration">');

    $generatorXml.cacheGeneral(cache, res);

    $generatorXml.cacheMemory(cache, res);

    $generatorXml.cacheQuery(cache, res);

    $generatorXml.cacheStore(cache, res);

    $generatorXml.cacheConcurrency(cache, res);

    $generatorXml.cacheRebalance(cache, res);

    $generatorXml.cacheServerNearCache(cache, res);

    $generatorXml.cacheStatistics(cache, res);

    $generatorXml.cacheMetadatas(cache.queryMetadata, cache.storeMetadata, res);

    res.endBlock('</bean>');

    return res;
};

// Generate caches configs.
$generatorXml.clusterCaches = function(caches, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (caches && caches.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="cacheConfiguration">');
        res.startBlock('<list>');

        for (var i = 0; i < caches.length; i++) {
            if (i > 0)
                res.line();

            $generatorXml.cache(caches[i], res);
        }

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cluster config.
$generatorXml.cluster = function (cluster, clientNearCfg) {
    if (cluster) {
        var res = $generatorCommon.builder();

        res.deep = 1;

        if (clientNearCfg) {
            res.startBlock('<bean id="nearCacheBean" class="org.apache.ignite.configuration.NearCacheConfiguration">');

            if (clientNearCfg.nearStartSize)
                $generatorXml.property(res, clientNearCfg, 'nearStartSize');

            if (clientNearCfg.nearEvictionPolicy && clientNearCfg.nearEvictionPolicy.kind)
                $generatorXml.evictionPolicy(res, clientNearCfg.nearEvictionPolicy, 'nearEvictionPolicy');

            res.endBlock('</bean>');

            res.line();
        }

        // Generate Ignite Configuration.
        res.startBlock('<bean class="org.apache.ignite.configuration.IgniteConfiguration">');

        if (clientNearCfg) {
            res.line('<property name="clientMode" value="true" />');

            res.line();
        }

        $generatorXml.clusterGeneral(cluster, res);

        $generatorXml.clusterAtomics(cluster, res);

        $generatorXml.clusterCommunication(cluster, res);

        $generatorXml.clusterDeployment(cluster, res);

        $generatorXml.clusterEvents(cluster, res);

        $generatorXml.clusterMarshaller(cluster, res);

        $generatorXml.clusterMetrics(cluster, res);

        $generatorXml.clusterP2p(cluster, res);

        $generatorXml.clusterSwap(cluster, res);

        $generatorXml.clusterTime(cluster, res);

        $generatorXml.clusterPools(cluster, res);

        $generatorXml.clusterTransactions(cluster, res);

        $generatorXml.clusterCaches(cluster.caches, res);

        res.endBlock('</bean>');

        // Build final XML:
        // 1. Add header.
        var xml = '<?xml version="1.0" encoding="UTF-8"?>\n\n';

        xml += '<!-- ' + $generatorCommon.mainComment() + ' -->\n';
        xml += '<beans xmlns="http://www.springframework.org/schema/beans"\n';
        xml += '       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n';
        xml += '       xmlns:util="http://www.springframework.org/schema/util"\n';
        xml += '       xsi:schemaLocation="http://www.springframework.org/schema/beans\n';
        xml += '                           http://www.springframework.org/schema/beans/spring-beans.xsd\n';
        xml += '                           http://www.springframework.org/schema/util\n';
        xml += '                           http://www.springframework.org/schema/util/spring-util.xsd">\n';

        // 2. Add external property file and all data sources.
        if (res.datasources.length > 0) {
            xml += '    <!-- Load external properties file. -->\n';
            xml += '    <bean id="placeholderConfig" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">\n';
            xml += '        <property name="location" value="classpath:secret.properties"/>\n';
            xml += '    </bean>\n\n';

            xml += '    <!-- Data source beans will be initialized from external properties file. -->\n';

            _.forEach(res.datasources, function (item) {
                var beanId = item.dataSourceBean;

                xml += '    <bean id= "' + beanId + '" class="' + item.className + '">\n';
                xml += '        <property name="URL" value="${' + beanId + '.jdbc.url}" />\n';
                xml += '        <property name="user" value="${' + beanId + '.jdbc.username}" />\n';
                xml += '        <property name="password" value="${' + beanId + '.jdbc.password}" />\n';
                xml += '    </bean>\n\n';
            });
        }

        // 3. Add main content.
        xml += res.join('');

        // 4. Add footer.
        xml += '</beans>\n';

        return xml;
    }

    return '';
};

// For server side we should export XML generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorXml;
}
