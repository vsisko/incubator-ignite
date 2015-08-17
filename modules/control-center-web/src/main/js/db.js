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

var config = require('./helpers/configuration-loader.js');

// Mongoose for mongodb.
var mongoose = require('mongoose'),
    Schema = mongoose.Schema,
    ObjectId = mongoose.Schema.Types.ObjectId,
    passportLocalMongoose = require('passport-local-mongoose');

var deepPopulate = require('mongoose-deep-populate')( mongoose);

// Connect to mongoDB database.
mongoose.connect(config.get('mongoDB:url'), {server: {poolSize: 4}});

// Define account model.
var AccountSchema = new Schema({
    username: String,
    email: String,
    lastLogin: Date,
    admin: Boolean,
    resetPasswordToken: String
});

AccountSchema.plugin(passportLocalMongoose, {usernameField: 'email', limitAttempts: true, lastLoginField: 'lastLogin',
    usernameLowerCase: true});

AccountSchema.set('toJSON', {
    transform: function(doc, ret) {
        return {
            _id: ret._id,
            email: ret.email,
            username: ret.username,
            admin: ret.admin,
            lastLogin: ret.lastLogin
        };
    }
});

exports.Account = mongoose.model('Account', AccountSchema);

// Define space model.
exports.Space = mongoose.model('Space', new Schema({
    name: String,
    owner: {type: ObjectId, ref: 'Account'},
    usedBy: [{
        permission: {type: String, enum: ['VIEW', 'FULL']},
        account: {type: ObjectId, ref: 'Account'}
    }]
}));

// Define cache type metadata model.
var CacheTypeMetadataSchema = new Schema({
    space: {type: ObjectId, ref: 'Space'},
    name: String,
    databaseSchema: String,
    databaseTable: String,
    keyType: String,
    valueType: String,
    keyFields: [{databaseName: String, databaseType: String, javaName: String, javaType: String}],
    valueFields: [{databaseName: String, databaseType: String, javaName: String, javaType: String}],
    queryFields: [{name: String, className: String}],
    ascendingFields: [{name: String, className: String}],
    descendingFields:  [{name: String, className: String}],
    textFields: [String],
    groups: [{name: String, fields: [{name: String, className: String, direction: Boolean}]}]
});

exports.CacheTypeMetadata = mongoose.model('CacheTypeMetadata', CacheTypeMetadataSchema);

// Define cache model.
var CacheSchema = new Schema({
    space: {type: ObjectId, ref: 'Space'},
    name: String,
    mode: {type: String, enum: ['PARTITIONED', 'REPLICATED', 'LOCAL']},
    atomicityMode: {type: String, enum: ['ATOMIC', 'TRANSACTIONAL']},

    backups: Number,
    memoryMode: {type: String, enum: ['ONHEAP_TIERED', 'OFFHEAP_TIERED', 'OFFHEAP_VALUES']},
    offHeapMaxMemory: Number,
    startSize: Number,
    swapEnabled: Boolean,

    evictionPolicy: {
        kind: {type: String, enum: ['LRU', 'RND', 'FIFO', 'Sorted']},
        LRU: {
            batchSize: Number,
            maxMemorySize: Number,
            maxSize: Number
        },
        RND: {
            maxSize: Number
        },
        FIFO: {
            batchSize: Number,
            maxMemorySize: Number,
            maxSize: Number
        },
        SORTED: {
            batchSize: Number,
            maxMemorySize: Number,
            maxSize: Number
        }
    },

    rebalanceMode: {type: String, enum: ['SYNC', 'ASYNC', 'NONE']},
    rebalanceThreadPoolSize: Number,
    rebalanceBatchSize: Number,
    rebalanceOrder: Number,
    rebalanceDelay: Number,
    rebalanceTimeout: Number,
    rebalanceThrottle: Number,

    storeMetadata: [{type: ObjectId, ref: 'CacheTypeMetadata'}],
    cacheStoreFactory: {
        kind: {
            type: String,
            enum: ['CacheJdbcPojoStoreFactory', 'CacheJdbcBlobStoreFactory', 'CacheHibernateBlobStoreFactory']
        },
        CacheJdbcPojoStoreFactory: {
            dataSourceBean: String,
            dialect: {
                type: String,
                enum: ['Oracle', 'DB2', 'SQLServer', 'MySQL', 'PosgreSQL', 'H2']
            }
        },
        CacheJdbcBlobStoreFactory: {
            user: String,
            dataSourceBean: String,
            initSchema: Boolean,
            createTableQuery: String,
            loadQuery: String,
            insertQuery: String,
            updateQuery: String,
            deleteQuery: String
        },
        CacheHibernateBlobStoreFactory: {
            hibernateProperties: [String]
        }
    },
    loadPreviousValue: Boolean,
    readThrough: Boolean,
    writeThrough: Boolean,

    writeBehindEnabled: Boolean,
    writeBehindBatchSize: Number,
    writeBehindFlushSize: Number,
    writeBehindFlushFrequency: Number,
    writeBehindFlushThreadCount: Number,

    invalidate: Boolean,
    defaultLockTimeout: Number,
    transactionManagerLookupClassName: String,

    sqlEscapeAll: Boolean,
    sqlOnheapRowCacheSize: Number,
    longQueryWarningTimeout: Number,
    queryMetadata: [{type: ObjectId, ref: 'CacheTypeMetadata'}],
    indexedTypes: [{keyClass: String, valueClass: String}],
    sqlFunctionClasses: [String],
    statisticsEnabled: Boolean,
    managementEnabled: Boolean,
    readFromBackup: Boolean,
    copyOnRead: Boolean,
    maxConcurrentAsyncOperations: Number,
    nearConfiguration: {
        nearStartSize: Number,
        nearEvictionPolicy: {
            kind: {type: String, enum: ['LRU', 'RND', 'FIFO', 'Sorted']},
            LRU: {
                batchSize: Number,
                maxMemorySize: Number,
                maxSize: Number
            },
            RND: {
                maxSize: Number
            },
            FIFO: {
                batchSize: Number,
                maxMemorySize: Number,
                maxSize: Number
            },
            SORTED: {
                batchSize: Number,
                maxMemorySize: Number,
                maxSize: Number
            }
        }
    }
});

exports.Cache = mongoose.model('Cache', CacheSchema);

// Define cluster schema.
var ClusterSchema = new Schema({
    space: {type: ObjectId, ref: 'Space'},
    name: String,
    discovery: {
        kind: {type: String, enum: ['Vm', 'Multicast', 'S3', 'Cloud', 'GoogleStorage', 'Jdbc', 'SharedFs']},
        Vm: {
            addresses: [String]
        },
        Multicast: {
            multicastGroup: String,
            multicastPort: Number,
            responseWaitTime: Number,
            addressRequestAttempts: Number,
            localAddress: String
        },
        S3: {
            bucketName: String
        },
        Cloud: {
            credential: String,
            credentialPath: String,
            identity: String,
            provider: String,
            regions: [String],
            zones:  [String]
        },
        GoogleStorage: {
            projectName: String,
            bucketName: String,
            serviceAccountP12FilePath: String,
            serviceAccountId: String,
            addrReqAttempts: String
        },
        Jdbc: {
            initSchema: Boolean
        },
        SharedFs: {
            path: String
        }
    },
    atomicConfiguration: {
        backups: Number,
        cacheMode: {type: String, enum: ['LOCAL', 'REPLICATED', 'PARTITIONED']},
        atomicSequenceReserveSize: Number
    },
    caches: [{type: ObjectId, ref: 'Cache'}],
    cacheSanityCheckEnabled: Boolean,
    clockSyncSamples: Number,
    clockSyncFrequency: Number,
    deploymentMode: {type: String, enum: ['PRIVATE', 'ISOLATED', 'SHARED', 'CONTINUOUS']},
    discoveryStartupDelay: Number,
    igfsThreadPoolSize: Number,
    includeEventTypes: [{
        type: String, enum: ['EVTS_CHECKPOINT', 'EVTS_DEPLOYMENT', 'EVTS_ERROR', 'EVTS_DISCOVERY',
            'EVTS_JOB_EXECUTION', 'EVTS_TASK_EXECUTION', 'EVTS_CACHE', 'EVTS_CACHE_REBALANCE', 'EVTS_CACHE_LIFECYCLE',
            'EVTS_CACHE_QUERY', 'EVTS_SWAPSPACE', 'EVTS_IGFS']
    }],
    managementThreadPoolSize: Number,
    marshaller: {
        kind: {type: String, enum: ['OptimizedMarshaller', 'JdkMarshaller']},
        OptimizedMarshaller: {
            poolSize: Number,
            requireSerializable: Boolean
        }
    },
    marshalLocalJobs: Boolean,
    marshallerCacheKeepAliveTime: Number,
    marshallerCacheThreadPoolSize: Number,
    metricsExpireTime: Number,
    metricsHistorySize: Number,
    metricsLogFrequency: Number,
    metricsUpdateFrequency: Number,
    networkTimeout: Number,
    networkSendRetryDelay: Number,
    networkSendRetryCount: Number,
    peerClassLoadingEnabled: Boolean,
    peerClassLoadingLocalClassPathExclude: [String],
    peerClassLoadingMissedResourcesCacheSize: Number,
    peerClassLoadingThreadPoolSize: Number,
    publicThreadPoolSize: Number,
    segmentCheckFrequency: Number,
    segmentationPolicy: {type: String, enum: ['RESTART_JVM', 'STOP', 'NOOP']},
    allSegmentationResolversPassRequired: Boolean,
    segmentationResolveAttempts: Number,
    swapSpaceSpi: {
        kind: {type: String, enum: ['FileSwapSpaceSpi']},
        FileSwapSpaceSpi: {
            baseDirectory: String,
            readStripesNumber: Number,
            maximumSparsity: Number,
            maxWriteQueueSize: Number,
            writeBufferSize: Number
        }
    },
    systemThreadPoolSize: Number,
    timeServerPortBase: Number,
    timeServerPortRange: Number,
    transactionConfiguration: {
        defaultTxConcurrency: {type: String, enum: ['OPTIMISTIC', 'PESSIMISTIC']},
        transactionIsolation: {type: String, enum: ['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE']},
        defaultTxTimeout: Number,
        pessimisticTxLogLinger: Number,
        pessimisticTxLogSize: Number,
        txSerializableEnabled: Boolean
    },
    waitForSegmentOnStart: Boolean
});

ClusterSchema.plugin(deepPopulate, {
    whitelist: [
        'caches',
        'caches.queryMetadata',
        'caches.storeMetadata'
    ]
});

// Define cluster model.
exports.Cluster = mongoose.model('Cluster', ClusterSchema);

// Define persistence schema.
var PersistenceSchema = new Schema({
    space: {type: ObjectId, ref: 'Space'},
    name: String,
    rdbms: {type: String, enum: ['oracle', 'db2', 'mssql', 'postgre', 'mysql', 'h2']},
    dbName: String,
    host: String,
    user: String,
    tables: [{
        use: Boolean,
        schemaName: String,
        tableName: String,
        keyClass: String,
        valueClass: String,
        columns: [{
            use: Boolean,
            pk: Boolean,
            ak: Boolean,
            notNull: Boolean,
            databaseName: String,
            databaseType: Number,
            javaName: String,
            javaType: String
        }]
    }]
});

// Define persistence model.
exports.Persistence = mongoose.model('Persistence', PersistenceSchema);

// Define persistence schema.
var NotebookSchema = new Schema({
    space: {type: ObjectId, ref: 'Space'},
    name: String,
    paragraph: [{
        query: String
    }]
});

// Define persistence model.
exports.Notebook = mongoose.model('Notebook', NotebookSchema);

exports.upsert = function (model, data, cb) {
    if (data._id) {
        var id = data._id;

        delete data._id;

        model.findOneAndUpdate({_id: id}, data, cb);
    }
    else
        new model(data).save(cb);
};

exports.mongoose = mongoose;
