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

var Query = require("./query").Query

/**
 * @this {SqlFieldsQuery}
 * @param {string} Sql query
 */
function SqlFieldsQuery(sql) {
    Query.apply(this, arguments);
    this._qryType = "SqlFields";
    this._sql = sql;
    this._arg = [];
    this._pageSz = 1;
}

SqlFieldsQuery.prototype = Query.prototype;

SqlFieldsQuery.prototype.constructor = SqlFieldsQuery;

/**
 * @this {SqlFieldsQuery}
 * @param args Arguments
 */
SqlFieldsQuery.prototype.setArguments = function(args) {
    this._arg = args;
}

/**
 * @this {SqlFieldsQuery}
 * @returns Sql query
 */
SqlFieldsQuery.prototype.query = function() {
    return this._sql;
}

/**
 * @this {SqlFieldsQuery}
 * @returns arguments
 */
SqlFieldsQuery.prototype.arguments = function() {
    return this._arg;
}

exports.SqlFieldsQuery = SqlFieldsQuery;
