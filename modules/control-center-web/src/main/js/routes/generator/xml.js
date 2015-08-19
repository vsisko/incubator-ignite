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

var _ = require('lodash');

var generatorCommon = require("./common");
var dataStructures = require("../../helpers/data-structures.js");

exports.generateClusterConfiguration = function (cluster, clientNearConfiguration) {
    var res = generatorCommon.builder();

    res.datasources = [];
    res.deep = 1;

    if (clientNearConfiguration) {
        res.startBlock('<bean id="nearCacheBean" class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (clientNearConfiguration.nearStartSize)
            addProperty(res, clientNearConfiguration, 'nearStartSize');

        if (clientNearConfiguration.nearEvictionPolicy && clientNearConfiguration.nearEvictionPolicy.kind)
            createEvictionPolicy(res, clientNearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');

        res.endBlock('</bean>');

        res.line();
    }

    // Generate Ignite Configuration.
    res.startBlock('<bean class="org.apache.ignite.configuration.IgniteConfiguration">');

    if (clientNearConfiguration) {
        res.line('<property name="clientMode" value="true" />');

        res.line();
    }

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
                res.line('<property name="initSchema" value="' + (generatorCommon.isDefined(d.Jdbc.initSchema) && d.Jdbc.initSchema) + '"/>');
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

        res.needEmptyLine = true
    }

    // Generate atomics group.
    addBeanWithProperties(res, cluster.atomicConfiguration, 'atomicConfiguration',
        generatorCommon.atomicConfiguration.className, generatorCommon.atomicConfiguration.fields);
    res.needEmptyLine = true;

    // Generate communication group.
    addProperty(res, cluster, 'networkTimeout');
    addProperty(res, cluster, 'networkSendRetryDelay');
    addProperty(res, cluster, 'networkSendRetryCount');
    addProperty(res, cluster, 'segmentCheckFrequency');
    addProperty(res, cluster, 'waitForSegmentOnStart');
    addProperty(res, cluster, 'discoveryStartupDelay');
    res.needEmptyLine = true;

    // Generate deployment group.
    addProperty(res, cluster, 'deploymentMode');
    res.needEmptyLine = true;

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

                var eventList = dataStructures.eventGroups[eventGroup];

                for (var k = 0; k < eventList.length; k++) {
                    res.line('<util:constant static-field="org.apache.ignite.events.EventType.' + eventList[k] + '"/>')
                }
            }

            res.endBlock('</array>');
        }

        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    // Generate marshaller group.
    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        var marshallerDesc = generatorCommon.marshallers[marshaller.kind];

        addBeanWithProperties(res, marshaller[marshaller.kind], 'marshaller', marshallerDesc.className, marshallerDesc.fields, true);
        res.needEmptyLine = true;
    }

    addProperty(res, cluster, 'marshalLocalJobs');
    addProperty(res, cluster, 'marshallerCacheKeepAliveTime');
    addProperty(res, cluster, 'marshallerCacheThreadPoolSize');
    res.needEmptyLine = true;

    // Generate metrics group.
    addProperty(res, cluster, 'metricsExpireTime');
    addProperty(res, cluster, 'metricsHistorySize');
    addProperty(res, cluster, 'metricsLogFrequency');
    addProperty(res, cluster, 'metricsUpdateFrequency');
    res.needEmptyLine = true;

    // Generate PeerClassLoading group.
    addProperty(res, cluster, 'peerClassLoadingEnabled');
    addListProperty(res, cluster, 'peerClassLoadingLocalClassPathExclude');
    addProperty(res, cluster, 'peerClassLoadingMissedResourcesCacheSize');
    addProperty(res, cluster, 'peerClassLoadingThreadPoolSize');
    res.needEmptyLine = true;

    // Generate swap group.
    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
        addBeanWithProperties(res, cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi',
            generatorCommon.swapSpaceSpi.className, generatorCommon.swapSpaceSpi.fields, true);

        res.needEmptyLine = true;
    }

    // Generate time group.
    addProperty(res, cluster, 'clockSyncSamples');
    addProperty(res, cluster, 'clockSyncFrequency');
    addProperty(res, cluster, 'timeServerPortBase');
    addProperty(res, cluster, 'timeServerPortRange');
    res.needEmptyLine = true;

    // Generate thread pools group.
    addProperty(res, cluster, 'publicThreadPoolSize');
    addProperty(res, cluster, 'systemThreadPoolSize');
    addProperty(res, cluster, 'managementThreadPoolSize');
    addProperty(res, cluster, 'igfsThreadPoolSize');
    res.needEmptyLine = true;

    // Generate transactions group.
    addBeanWithProperties(res, cluster.transactionConfiguration, 'transactionConfiguration',
        generatorCommon.transactionConfiguration.className, generatorCommon.transactionConfiguration.fields);
    res.needEmptyLine = true;

    // Generate caches configs.
    if (cluster.caches && cluster.caches.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="cacheConfiguration">');
        res.startBlock('<list>');

        for (var i = 0; i < cluster.caches.length; i++) {
            if (i > 0)
                res.line();

            var cache = cluster.caches[i];

            generateCacheConfiguration(res, cache);
        }

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    res.endBlock('</bean>');

    // Build final XML:
    // 1. Add header.
    var xml = '<?xml version="1.0" encoding="UTF-8"?>\n\n';

    xml += '<!-- ' + generatorCommon.mainComment() + ' -->\n';
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

function createEvictionPolicy(res, evictionPolicy, propertyName) {
    if (evictionPolicy && evictionPolicy.kind) {
        var e = generatorCommon.evictionPolicies[evictionPolicy.kind];

        var obj = evictionPolicy[evictionPolicy.kind.toUpperCase()];

        addBeanWithProperties(res, obj, propertyName, e.className, e.fields, true);
    }
}

function addCacheTypeMetadataDatabaseFields(res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        res.startBlock('<property name="' + fieldProperty + '">');

        res.startBlock('<list>');

        _.forEach(fields, function (field) {
            res.startBlock('<bean class="org.apache.ignite.cache.CacheTypeFieldMetadata">');

            addProperty(res, field, 'databaseName');

            res.startBlock('<property name="databaseType">');
            res.line('<util:constant static-field="java.sql.Types.' + field.databaseType + '"/>');
            res.endBlock('</property>');

            addProperty(res, field, 'javaName');

            addClassNameProperty(res, field, 'javaType');

            res.endBlock('</bean>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');
    }
}

function addCacheTypeMetadataQueryFields(res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        res.startBlock('<property name="' + fieldProperty + '">');

        res.startBlock('<map>');

        _.forEach(fields, function (field) {
            addElement(res, 'entry', 'key', field.name, 'value', generatorCommon.javaBuildInClass(field.className));
        });

        res.endBlock('</map>');

        res.endBlock('</property>');
    }
}

function addCacheTypeMetadataGroups(res, meta) {
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
                    res.line('<constructor-arg value="' + generatorCommon.javaBuildInClass(field.className) + '"/>');
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

function generateCacheTypeMetadataConfiguration(res, meta) {
    if (!res)
        res = generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.cache.CacheTypeMetadata">');

    var kind = meta.kind;

    var keyType = addClassNameProperty(res, meta, 'keyType');

    addProperty(res, meta, 'valueType');

    if (kind != 'query') {
        addProperty(res, meta, 'databaseSchema');
        addProperty(res, meta, 'databaseTable');

        if (!generatorCommon.isJavaBuildInClass(keyType))
            addCacheTypeMetadataDatabaseFields(res, meta, 'keyFields');

        addCacheTypeMetadataDatabaseFields(res, meta, 'valueFields');
    }

    if (kind != 'store') {
        addCacheTypeMetadataQueryFields(res, meta, 'queryFields');
        addCacheTypeMetadataQueryFields(res, meta, 'ascendingFields');
        addCacheTypeMetadataQueryFields(res, meta, 'descendingFields');

        addListProperty(res, meta, 'textFields');

        addCacheTypeMetadataGroups(res, meta);
    }

    res.endBlock('</bean>');

    return res;
}

function generateCacheConfiguration(res, cacheCfg) {
    if (!res)
        res = generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.configuration.CacheConfiguration">');

    addProperty(res, cacheCfg, 'name');

    res.needEmptyLine = true;

    var cacheMode = addProperty(res, cacheCfg, 'mode', 'cacheMode');

    addProperty(res, cacheCfg, 'atomicityMode');

    if (cacheMode == 'PARTITIONED')
        addProperty(res, cacheCfg, 'backups');

    addProperty(res, cacheCfg, 'readFromBackup');

    addProperty(res, cacheCfg, 'startSize');

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'memoryMode');
    addProperty(res, cacheCfg, 'offHeapMaxMemory');
    addProperty(res, cacheCfg, 'swapEnabled');
    addProperty(res, cacheCfg, 'copyOnRead');

    res.needEmptyLine = true;

    createEvictionPolicy(res, cacheCfg.evictionPolicy, 'evictionPolicy');

    res.needEmptyLine = true;

    if (cacheCfg.nearCacheEnabled) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="nearConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (cacheCfg.nearConfiguration && cacheCfg.nearConfiguration.nearStartSize)
            addProperty(res, cacheCfg.nearConfiguration, 'nearStartSize');

        if (cacheCfg.nearConfiguration && cacheCfg.nearConfiguration.nearEvictionPolicy.kind)
            createEvictionPolicy(res, cacheCfg.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');

        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'sqlEscapeAll');
    addProperty(res, cacheCfg, 'sqlOnheapRowCacheSize');
    addProperty(res, cacheCfg, 'longQueryWarningTimeout');

    if (cacheCfg.indexedTypes && cacheCfg.indexedTypes.length > 0) {
        res.startBlock('<property name="indexedTypes">');
        res.startBlock('<list>');

        for (var i = 0; i < cacheCfg.indexedTypes.length; i++) {
            var pair = cacheCfg.indexedTypes[i];

            res.line('<value>' + generatorCommon.javaBuildInClass(pair.keyClass) + '</value>');
            res.line('<value>' + generatorCommon.javaBuildInClass(pair.valueClass) + '</value>');
        }

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    addListProperty(res, cacheCfg, 'sqlFunctionClasses', 'array');

    res.needEmptyLine = true;

    if (cacheMode != 'LOCAL') {
        addProperty(res, cacheCfg, 'rebalanceMode');
        addProperty(res, cacheCfg, 'rebalanceThreadPoolSize');
        addProperty(res, cacheCfg, 'rebalanceBatchSize');
        addProperty(res, cacheCfg, 'rebalanceOrder');
        addProperty(res, cacheCfg, 'rebalanceDelay');
        addProperty(res, cacheCfg, 'rebalanceTimeout');
        addProperty(res, cacheCfg, 'rebalanceThrottle');

        res.needEmptyLine = true;
    }

    if (cacheCfg.cacheStoreFactory && cacheCfg.cacheStoreFactory.kind) {
        var storeFactory = cacheCfg.cacheStoreFactory[cacheCfg.cacheStoreFactory.kind];
        var data = generatorCommon.storeFactories[cacheCfg.cacheStoreFactory.kind];

        addBeanWithProperties(res, storeFactory, 'cacheStoreFactory', data.className, data.fields, true);

        if (storeFactory.dialect) {
            if (_.findIndex(res.datasources, function (ds) {
                    return ds.dataSourceBean == storeFactory.dataSourceBean;
                }) < 0) {
                res.datasources.push({
                    dataSourceBean: storeFactory.dataSourceBean,
                    className: generatorCommon.dataSources[storeFactory.dialect]
                });
            }
        }
    }

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'loadPreviousValue');
    addProperty(res, cacheCfg, 'readThrough');
    addProperty(res, cacheCfg, 'writeThrough');

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'invalidate');
    addProperty(res, cacheCfg, 'defaultLockTimeout');
    addProperty(res, cacheCfg, 'transactionManagerLookupClassName');

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'writeBehindEnabled');
    addProperty(res, cacheCfg, 'writeBehindBatchSize');
    addProperty(res, cacheCfg, 'writeBehindFlushSize');
    addProperty(res, cacheCfg, 'writeBehindFlushFrequency');
    addProperty(res, cacheCfg, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'statisticsEnabled');
    addProperty(res, cacheCfg, 'managementEnabled');

    res.needEmptyLine = true;

    addProperty(res, cacheCfg, 'maxConcurrentAsyncOperations');

    // Generate cache type metadata configs.
    if ((cacheCfg.queryMetadata && cacheCfg.queryMetadata.length > 0) ||
        (cacheCfg.storeMetadata && cacheCfg.storeMetadata.length > 0)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="typeMetadata">');
        res.startBlock('<list>');

        var metaNames = [];

        if (cacheCfg.queryMetadata && cacheCfg.queryMetadata.length > 0) {
            _.forEach(cacheCfg.queryMetadata, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    generateCacheTypeMetadataConfiguration(res, meta);
                }
            });
        }

        if (cacheCfg.storeMetadata && cacheCfg.storeMetadata.length > 0) {
            _.forEach(cacheCfg.storeMetadata, function (meta) {
                if (!_.contains(metaNames, meta.name)) {
                    metaNames.push(meta.name);

                    generateCacheTypeMetadataConfiguration(res, meta);
                }
            });
        }

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    res.endBlock('</bean>');

    return res;
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

    if (generatorCommon.isDefined(val))
        addElement(res, 'property', 'name', setterName ? setterName : propName, 'value', escapeAttr(val));

    return val;
}

function addClassNameProperty(res, obj, propName) {
    var val = obj[propName];

    if (generatorCommon.isDefined(val))
        addElement(res, 'property', 'name', propName, 'value', generatorCommon.javaBuildInClass(val));

    return val;
}

function addBeanWithProperties(res, bean, beanPropName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && generatorCommon.hasProperty(bean, props)) {
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
                            res.line('<bean class="' + generatorCommon.knownClasses[bean[propName]].className + '"/>');
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

function escapeAttr(s) {
    if (typeof(s) != 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;');
}

function escape(s) {
    if (typeof(s) != 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
