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

function _escape(s) {
    if (typeof(s) != 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function _escapeAttr(s) {
    if (typeof(s) != 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
}

function _addProperty(res, obj, propName, setterName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val))
        _addElement(res, 'property', 'name', setterName ? setterName : propName, 'value', _escapeAttr(val));

    return val;
}

function _addClassNameProperty(res, obj, propName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val))
        _addElement(res, 'property', 'name', propName, 'value', $generatorCommon.javaBuildInClass(val));

    return val;
}

function _addListProperty(res, obj, propName, listType, rowFactory) {
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

function _addBeanWithProperties(res, bean, beanPropName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && $commonUtils.hasProperty(bean, props)) {
        res.emptyLineIfNeeded();
        res.startBlock('<property name="' + beanPropName + '">');
        res.startBlock('<bean class="' + beanClass + '">');

        for (var propName in props) {
            if (props.hasOwnProperty(propName)) {
                var descr = props[propName];

                if (descr) {
                    if (descr.type == 'list') {
                        _addListProperty(res, bean, propName, descr.setterName);
                    }
                    else if (descr.type == 'className') {
                        if (bean[propName]) {
                            res.startBlock('<property name="' + propName + '">');
                            res.line('<bean class="' + $generatorCommon.KNOWN_CLASSES[bean[propName]].className + '"/>');
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
                                    res.line('<prop key="' + _escapeAttr(nameAndValue.substring(0, eqIndex)) + '">' +
                                        escape(nameAndValue.substr(eqIndex + 1)) + '</prop>');
                                }
                            }

                            res.endBlock('</props>');
                            res.endBlock('</property>');
                        }
                    }
                    else
                        _addProperty(res, bean, propName, descr.setterName);
                }
                else
                    _addProperty(res, bean, propName);
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

function _createEvictionPolicy(res, evictionPolicy, propertyName) {
    if (evictionPolicy && evictionPolicy.kind) {
        var e = $generatorCommon.evictionPolicies[evictionPolicy.kind];

        var obj = evictionPolicy[evictionPolicy.kind.toUpperCase()];

        _addBeanWithProperties(res, obj, propertyName, e.className, e.fields, true);
    }
}

function _addCacheTypeMetadataDatabaseFields(res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        res.startBlock('<property name="' + fieldProperty + '">');

        res.startBlock('<list>');

        _.forEach(fields, function (field) {
            res.startBlock('<bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">');

            _addProperty(res, field, 'databaseName');

            res.startBlock('<property name="databaseType">');
            res.line('<util:constant static-field="java.sql.Types.' + field.databaseType + '"/>');
            res.endBlock('</property>');

            _addProperty(res, field, 'javaName');

            _addClassNameProperty(res, field, 'javaType');

            res.endBlock('</bean>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');
    }
}

function _addCacheTypeMetadataQueryFields(res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        res.startBlock('<property name="' + fieldProperty + '">');

        res.startBlock('<map>');

        _.forEach(fields, function (field) {
            _addElement(res, 'entry', 'key', field.name, 'value', $generatorCommon.javaBuildInClass(field.className));
        });

        res.endBlock('</map>');

        res.endBlock('</property>');
    }
}

function _addCacheTypeMetadataGroups(res, meta) {
    var groups = meta.groups;

    if (groups && groups.length > 0) {
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
                    res.line('<constructor-arg value="' + $generatorCommon.javaBuildInClass(field.className) + '"/>');
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
    }
}

function _generateCacheTypeMetadataConfiguration(res, meta) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.cache.CacheTypeMetadata">');

    var kind = meta.kind;

    var keyType = _addClassNameProperty(res, meta, 'keyType');

    _addProperty(res, meta, 'valueType');

    if (kind != 'query') {
        _addProperty(res, meta, 'databaseSchema');
        _addProperty(res, meta, 'databaseTable');

        if (!$dataStructures.isJavaBuildInClass(keyType))
            _addCacheTypeMetadataDatabaseFields(res, meta, 'keyFields');

        _addCacheTypeMetadataDatabaseFields(res, meta, 'valueFields');
    }

    if (kind != 'store') {
        _addCacheTypeMetadataQueryFields(res, meta, 'queryFields');
        _addCacheTypeMetadataQueryFields(res, meta, 'ascendingFields');
        _addCacheTypeMetadataQueryFields(res, meta, 'descendingFields');

        _addListProperty(res, meta, 'textFields');

        _addCacheTypeMetadataGroups(res, meta);
    }

    res.endBlock('</bean>');

    return res;
}

function _addElement(res, tag, attr1, val1, attr2, val2) {
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

$generatorXml = {};

// Generate discovery.
$generatorXml.general = function (cluster, caches, res) {
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

                _addProperty(res, d.Multicast, 'multicastGroup');
                _addProperty(res, d.Multicast, 'multicastPort');
                _addProperty(res, d.Multicast, 'responseWaitTime');
                _addProperty(res, d.Multicast, 'addressRequestAttempts');
                _addProperty(res, d.Multicast, 'localAddress');

                res.endBlock('</bean>');

                break;

            case 'Vm':
                if (d.Vm.addresses.length > 0) {
                    res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">');

                    _addListProperty(res, d.Vm, 'addresses');

                    res.endBlock('</bean>');
                }
                else {
                    res.line('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder"/>');
                }

                break;

            case 'S3':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder">');

                if (d.S3 && d.S3.bucketName)
                    res.line('<property name="bucketName" value="' + _escapeAttr(d.S3.bucketName) + '" />');

                res.endBlock('</bean>');

                break;

            case 'Cloud':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder">');

                _addProperty(res, d.Cloud, 'credential');
                _addProperty(res, d.Cloud, 'credentialPath');
                _addProperty(res, d.Cloud, 'identity');
                _addProperty(res, d.Cloud, 'provider');
                _addListProperty(res, d.Cloud, 'regions');
                _addListProperty(res, d.Cloud, 'zones');

                res.endBlock('</bean>');

                break;

            case 'GoogleStorage':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder">');

                _addProperty(res, d.GoogleStorage, 'projectName');
                _addProperty(res, d.GoogleStorage, 'bucketName');
                _addProperty(res, d.GoogleStorage, 'serviceAccountP12FilePath');
                _addProperty(res, d.GoogleStorage, 'serviceAccountId');

                //if (d.GoogleStorage.addrReqAttempts) todo ????
                //    res.line('<property name="serviceAccountP12FilePath" value="' + _escapeAttr(d.GoogleStorage.addrReqAttempts) + '"/>');

                res.endBlock('</bean>');

                break;

            case 'Jdbc':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder">');
                res.line('<property name="initSchema" value="' + ($commonUtils.isDefined(d.Jdbc.initSchema) && d.Jdbc.initSchema) + '"/>');
                res.endBlock('</bean>');

                break;

            case 'SharedFs':
                if (d.SharedFs.path) {
                    res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder">');
                    _addProperty(res, d.SharedFs, 'path');
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
};

// Generate atomics group.
$generatorXml.atomics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var atomicCfg = $generatorCommon.ATOMIC_CONFIGURATION;

    _addBeanWithProperties(res, cluster.atomicConfiguration, 'atomicConfiguration', atomicCfg.className, atomicCfg.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate communication group.
$generatorXml.communication = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    _addProperty(res, cluster, 'networkTimeout');
    _addProperty(res, cluster, 'networkSendRetryDelay');
    _addProperty(res, cluster, 'networkSendRetryCount');
    _addProperty(res, cluster, 'segmentCheckFrequency');
    _addProperty(res, cluster, 'waitForSegmentOnStart');
    _addProperty(res, cluster, 'discoveryStartupDelay');

    res.needEmptyLine = true;

    return res;
};

// Generate deployment group.
$generatorXml.deployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    _addProperty(res, cluster, 'deploymentMode');

    res.needEmptyLine = true;

    return res;
};

// Generate events group.
$generatorXml.events = function (cluster, res) {
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
$generatorXml.marshaller = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        var marshallerDesc = $generatorCommon.MARSHALLERS[marshaller.kind];

        _addBeanWithProperties(res, marshaller[marshaller.kind], 'marshaller', marshallerDesc.className, marshallerDesc.fields, true);

        res.needEmptyLine = true;
    }

    _addProperty(res, cluster, 'marshalLocalJobs');
    _addProperty(res, cluster, 'marshallerCacheKeepAliveTime');
    _addProperty(res, cluster, 'marshallerCacheThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorXml.metrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    _addProperty(res, cluster, 'metricsExpireTime');
    _addProperty(res, cluster, 'metricsHistorySize');
    _addProperty(res, cluster, 'metricsLogFrequency');
    _addProperty(res, cluster, 'metricsUpdateFrequency');

    res.needEmptyLine = true;

    return res;
};

// Generate PeerClassLoading group.
$generatorXml.p2p = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    _addProperty(res, cluster, 'peerClassLoadingEnabled');
    _addListProperty(res, cluster, 'peerClassLoadingLocalClassPathExclude');
    _addProperty(res, cluster, 'peerClassLoadingMissedResourcesCacheSize');
    _addProperty(res, cluster, 'peerClassLoadingThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate swap group.
$generatorXml.swap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var swapSpaceSpi = $generatorCommon.SWAP_SPACE_SPI;

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
        _addBeanWithProperties(res, cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi',
            swapSpaceSpi.className, swapSpaceSpi.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate time group.
$generatorXml.time = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    _addProperty(res, cluster, 'clockSyncSamples');
    _addProperty(res, cluster, 'clockSyncFrequency');
    _addProperty(res, cluster, 'timeServerPortBase');
    _addProperty(res, cluster, 'timeServerPortRange');

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorXml.pools = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    _addProperty(res, cluster, 'publicThreadPoolSize');
    _addProperty(res, cluster, 'systemThreadPoolSize');
    _addProperty(res, cluster, 'managementThreadPoolSize');
    _addProperty(res, cluster, 'igfsThreadPoolSize');
    res.needEmptyLine = true;

    return res;
};

// Generate transactions group.
$generatorXml.transactions = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var trnCfg = $generatorCommon.TRANSACTION_CONFIGURATION;

    _addBeanWithProperties(res, cluster.transactionConfiguration, 'transactionConfiguration', trnCfg.className, trnCfg.fields);

    res.needEmptyLine = true;

    return res;
};

// Generate caches configs.
$generatorXml.cache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.configuration.CacheConfiguration">');

    _addProperty(res, cache, 'name');

    res.needEmptyLine = true;

    var cacheMode = _addProperty(res, cache, 'mode', 'cacheMode');

    _addProperty(res, cache, 'atomicityMode');

    if (cacheMode == 'PARTITIONED')
        _addProperty(res, cache, 'backups');

    _addProperty(res, cache, 'readFromBackup');

    _addProperty(res, cache, 'startSize');

    res.needEmptyLine = true;

    _addProperty(res, cache, 'memoryMode');
    _addProperty(res, cache, 'offHeapMaxMemory');
    _addProperty(res, cache, 'swapEnabled');
    _addProperty(res, cache, 'copyOnRead');

    res.needEmptyLine = true;

    _createEvictionPolicy(res, cache.evictionPolicy, 'evictionPolicy');

    res.needEmptyLine = true;

    if (cache.nearCacheEnabled) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="nearConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (cache.nearConfiguration && cache.nearConfiguration.nearStartSize)
            _addProperty(res, cache.nearConfiguration, 'nearStartSize');

        if (cache.nearConfiguration && cache.nearConfiguration.nearEvictionPolicy.kind)
            _createEvictionPolicy(res, cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');

        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    res.needEmptyLine = true;

    _addProperty(res, cache, 'sqlEscapeAll');
    _addProperty(res, cache, 'sqlOnheapRowCacheSize');
    _addProperty(res, cache, 'longQueryWarningTimeout');

    if (cache.indexedTypes && cache.indexedTypes.length > 0) {
        res.startBlock('<property name="indexedTypes">');
        res.startBlock('<list>');

        for (var i = 0; i < cache.indexedTypes.length; i++) {
            var pair = cache.indexedTypes[i];

            res.line('<value>' + $generatorCommon.fullClassName(pair.keyClass) + '</value>');
            res.line('<value>' + $generatorCommon.fullClassName(pair.valueClass) + '</value>');
        }

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    _addListProperty(res, cache, 'sqlFunctionClasses', 'array');

    res.needEmptyLine = true;

    if (cacheMode != 'LOCAL') {
        _addProperty(res, cache, 'rebalanceMode');
        _addProperty(res, cache, 'rebalanceThreadPoolSize');
        _addProperty(res, cache, 'rebalanceBatchSize');
        _addProperty(res, cache, 'rebalanceOrder');
        _addProperty(res, cache, 'rebalanceDelay');
        _addProperty(res, cache, 'rebalanceTimeout');
        _addProperty(res, cache, 'rebalanceThrottle');

        res.needEmptyLine = true;
    }

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

        var storeFactoryDesc = $generatorCommon.STORE_FACTORIES[cache.cacheStoreFactory.kind];

        _addBeanWithProperties(res, storeFactory, 'cacheStoreFactory', storeFactoryDesc.className, storeFactoryDesc.fields, true);

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
    }

    res.needEmptyLine = true;

    _addProperty(res, cache, 'loadPreviousValue');
    _addProperty(res, cache, 'readThrough');
    _addProperty(res, cache, 'writeThrough');

    res.needEmptyLine = true;

    _addProperty(res, cache, 'invalidate');
    _addProperty(res, cache, 'defaultLockTimeout');
    _addProperty(res, cache, 'transactionManagerLookupClassName');

    res.needEmptyLine = true;

    _addProperty(res, cache, 'writeBehindEnabled');
    _addProperty(res, cache, 'writeBehindBatchSize');
    _addProperty(res, cache, 'writeBehindFlushSize');
    _addProperty(res, cache, 'writeBehindFlushFrequency');
    _addProperty(res, cache, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    _addProperty(res, cache, 'statisticsEnabled');
    _addProperty(res, cache, 'managementEnabled');

    res.needEmptyLine = true;

    _addProperty(res, cache, 'maxConcurrentAsyncOperations');

    // Generate cache type metadata configs.
    if ((cache.queryMetadata && cache.queryMetadata.length > 0) ||
        (cache.storeMetadata && cache.storeMetadata.length > 0)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="typeMetadata">');
        res.startBlock('<list>');

        var metaNames = [];

        if (cache.queryMetadata && cache.queryMetadata.length > 0) {
            _.forEach(cache.queryMetadata, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    _generateCacheTypeMetadataConfiguration(res, meta);
                }
            });
        }

        if (cache.storeMetadata && cache.storeMetadata.length > 0) {
            _.forEach(cache.storeMetadata, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    _generateCacheTypeMetadataConfiguration(res, meta);
                }
            });
        }

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    res.endBlock('</bean>');

    return res;
};

// Generate caches configs.
$generatorXml.caches = function(caches, res) {
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

$generatorXml.clusterConfiguration = function (cluster, clientNearConfiguration) {
    var res = $generatorCommon.builder();

    if (clientNearConfiguration) {
        res.startBlock('<bean id="nearCacheBean" class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (clientNearConfiguration.nearStartSize)
            _addProperty(res, clientNearConfiguration, 'nearStartSize');

        if (clientNearConfiguration.nearEvictionPolicy && clientNearConfiguration.nearEvictionPolicy.kind)
            _createEvictionPolicy(res, clientNearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');

        res.endBlock('</bean>');

        res.line();
    }

    // Generate Ignite Configuration.
    res.startBlock('<bean class="org.apache.ignite.configuration.IgniteConfiguration">');

    if (clientNearConfiguration) {
        res.line('<property name="clientMode" value="true" />');

        res.line();
    }

    $generatorXml.general(cluster, res);

    $generatorXml.atomics(cluster, res);

    $generatorXml.communication(cluster, res);

    $generatorXml.deployment(cluster, res);

    $generatorXml.events(cluster, res);

    $generatorXml.marshaller(cluster, res);

    $generatorXml.metrics(cluster, res);

    $generatorXml.p2p(cluster, res);

    $generatorXml.swap(cluster, res);

    $generatorXml.time(cluster, res);

    $generatorXml.pools(cluster, res);

    $generatorXml.transactions(cluster, res);

    $generatorXml.caches(cluster.caches, res);

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
};

if (typeof window === 'undefined') {
    module.exports = $generatorXml;
}
