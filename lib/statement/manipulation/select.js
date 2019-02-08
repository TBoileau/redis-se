/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    moment = require("moment"),
    async = require("async"),
    uniqid = require('uniqid'),
    multisort = require("multisort"),
    _ = require("underscore"),
    parser = require("node-sqlparser").parse;
module.exports = {
    cachedKeys: [],
    redis: null,
    database: null,
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        this.redis = redis;
        this.database = database;
        this.querying(queryString,function(reply){
            queryResponse(reply);
        });
    },
    querying: function(queryString,queryResponse)
    {
        var query = null;
        var global_aggregation = false;
        var self = this;
        var columns = [];
        var afterGroupBy = _.after(1,function(){
            self.parsePredicates(query, query.where, function (key) {
                self.redis.smembers(key, function (err, members) {
                    var results = [];
                    async.each(members, function (id, callback1) {
                        var result = {};
                        async.forEachOf(query.columns, function (column,index, callback) {
                            if(column.expr.type=="column_ref"){
                                var select = column.expr;
                            }else if(column.expr.type=="aggr_func"){
                                var select = column.expr.args.expr;
                            }
                            self.parseSelect(query, select, id, function (value) {
                                var join = _.findWhere(query.from, {as: select.table});
                                self.redis.hget(keyGenerator.columnHashKeySchema(self.database,join.table,select.column),"type", function(err,type){
                                    if(type == "int"){
                                        result[index] = parseInt(value);
                                    }else if(type == "decimal"){
                                        result[index] = parseFloat(value);
                                    }else if(type == "string"){
                                        result[index] = value;
                                    }else if(type == "date"){
                                        result[index] = parseInt(value);
                                    }else if(type == "datetime"){
                                        result[index] = parseInt(value);
                                    }else if(type == "array"){
                                        result[index] = value;
                                    }
                                    callback();
                                });
                            });
                        }, function () {
                            results.push(result);
                            callback1();
                        });
                    }, function () {
                        var aggregates = _.filter(query.columns,function(column){
                            return column.expr.type == "aggr_func";
                        });
                        if(aggregates.length > 0 && aggregates.length == query.columns.length) {
                            var tmp_results = {};
                            global_aggregation = true;
                            async.forEachOf(aggregates, function (aggregate,key, callback2) {
                                if (aggregate.expr.name == "COUNT") {
                                    tmp_results[key] = results.length;
                                    callback2();
                                } else {
                                    var aggregate_value = 0;
                                    async.eachOf(results, function (result, key, callback3) {
                                        aggregate_value += parseFloat(result[_.indexOf(query.columns,aggregate)]);
                                        callback3();
                                    }, function () {
                                        if (aggregate.expr.name == "AVG") {
                                            aggregate_value = aggregate_value / results.length;
                                        }
                                        tmp_results[key] = aggregate_value;
                                        callback2();
                                    });
                                }
                            }, function () {
                                results = [];
                                results.push(tmp_results);
                                sort();
                            })
                        }else if(aggregates.length != query.columns.length && aggregates.length > 0){
                            async.forEachOf(results,function(result,key,callback){
                                var where = "";
                                async.each(query.groupby, function(gb, groupByCallback){
                                    where = where == "" ? result[gb.index] : where + "#" + result[gb.index];
                                    groupByCallback();
                                }, function(){
                                    results[key]["group_field"] = where;
                                    callback();
                                });
                            },function(){
                                var groupedResults = _.groupBy(results, "group_field");

                                var tempResults = [];

                                async.eachOf(groupedResults, function(group,key,groupCallback){
                                    var newResult = group[0];
                                    delete newResult["group_field"];
                                    async.forEachOf(query.columns, function(column,index,mapCallback){
                                        if(column.expr.type == "aggr_func"){
                                            var aggreResults = _.pluck(group,index);
                                            if (column.expr.name.toLowerCase() == "avg") {
                                                newResult[index] = _.reduce(aggreResults, function(total,sum){return parseFloat(sum) + total;},0)/aggreResults.length;
                                            }else if (column.expr.name.toLowerCase() == "sum") {
                                                newResult[index] = _.reduce(aggreResults, function(total,sum){return parseFloat(sum) + total;},0);
                                            }else if (column.expr.name.toLowerCase() == "count") {
                                                newResult[index] = aggreResults.length;
                                            }
                                        }
                                        mapCallback();
                                    }, function(){
                                        tempResults.push(newResult);
                                        groupCallback();
                                    });
                                }, function(){
                                    results = tempResults;
                                    sort();
                                });
                            });
                        }else{
                            sort();
                        }

                        function sort(){
                            if(!global_aggregation){
                                if(query.orderby != null){
                                    multisort(results, _.map(query.orderby, function (orderby) {
                                        return (orderby.type == "DESC" ? "~" : "") + orderby.expr.index;
                                    }));
                                }
                                if(query.limit != null) {
                                    if (parseInt(query.limit[1].value) > 0) {
                                        results = results.splice(query.limit[0].value, query.limit[1].value);
                                    } else {
                                        results = results.slice(query.limit[0].value);
                                    }
                                }
                            }
                            async.each(self.cachedKeys, function(key){
                                self.redis.del(key);
                            });
                            queryResponse(response.send(queryString,true,{columns:columns,results:results}));
                        }
                    });
                });
            });
        });

        var afterOrderBy = _.after(1, function(){
            async.forEachOf(query.groupby, function(groupby,key,callback){
                if(groupby.table == "") {
                    groupby.table = query.from[0].as;
                    query.groupby[key].table = query.from[0].as;
                    query.groupby[key].column = groupby.table+"_"+groupby.column;
                }
                query.groupby[key].index = _.indexOf(query.columns,_.findWhere(query.columns,{ as: groupby.column }));
                callback();
            }, function(){
                afterGroupBy();
            });
        });

        var afterColumns = _.after(1, function(){
            if(query.orderby != null)
            {
                async.forEachOf(query.orderby, function(orderby,key,callback){
                    if(_.isUndefined(_.findWhere(query.columns,{ as: orderby.expr.column }))){
                        if(orderby.expr.table == "") {
                            orderby.expr.table = query.from[0].as;
                            query.orderby[key].expr.table = query.from[0].as;
                            query.orderby[key].expr.column = orderby.expr.table+"_"+orderby.expr.column;
                        }
                    }
                    query.orderby[key].expr.index = _.indexOf(query.columns,_.findWhere(query.columns,{ as: orderby.expr.column }));
                    callback();
                }, function(){
                    afterOrderBy();
                });
            }else{
                afterOrderBy();
            }
        });

        var afterFrom = _.after(1, function(){
            if(query.columns == "*"){
                query.columns = [];
                async.each(query.from, function(from,callback){
                    self.redis.smembers(keyGenerator.columnsListKeySchema(self.database,from.table), function(err,members){
                        async.each(members, function(member,callback1){
                            query.columns.push({
                                expr: {
                                    type: 'column_ref',
                                    table: from.as,
                                    column: member
                                },
                                as: from.as+"_"+member
                            });
                            columns.push({
                                table: from.table,
                                column: member,
                                as: from.as+"_"+member
                            });
                            callback1();
                        }, function(){
                            callback();
                        });
                    });
                }, function(){
                    afterColumns();
                });
            }else{
                if(_.filter(query.columns, function(column){ return column.expr.type == "aggr_func" ? column.expr.args.expr.table == "" : column.expr.table == ""; }).length > 0 && query.from.length > 1){
                    queryResponse(response.send(queryString, false, _.filter(query.columns, function(column){ return column.expr.table == ""; }), 121));
                }else{
                    async.forEachOf(query.columns, function (column,key, callback) {
                        if(column.expr.type == "aggr_func") {
                            if(column.expr.args.expr.table == "") {
                                column.expr.args.expr.table = query.from[0].as;
                                query.columns[key].expr.args.expr.table = query.from[0].as;
                            }
                            if(column.as == null){
                                query.columns[key].as = column.expr.name+"_"+column.expr.args.expr.table+"_"+column.expr.args.expr.column;
                            }
                            columns.push({
                                table: query.from[0].table,
                                column: column.expr.args.expr.column,
                                as: query.columns[key].as
                            });
                        }else{
                            if(column.expr.table == "") {
                                column.expr.table = query.from[0].as;
                                query.columns[key].expr.table = query.from[0].as;
                            }
                            if(column.as == null){
                                query.columns[key].as = column.expr.table+"_"+column.expr.column;
                            }
                            columns.push({
                                table: query.from[0].table,
                                column: column.expr.column,
                                as: query.columns[key].as
                            });
                        }
                        callback();
                    },function(){
                        afterColumns();
                    });
                }
            }
        });

        var afterTry = _.after(1, function(){
            async.forEachOf(query.from, function(from,key,callback){
                if(from.as == null){
                    query.from[key].as = key+"_"+query.from[key].table;
                }
                callback();
            }, function(){
                afterFrom();
            });
        });

        try{
            query = parser(queryString);
            afterTry();
        }catch(e){
            queryResponse(response.send(queryString,false,{},2));
        }

    },
    recursivePredicateKey: function (query, predicate, closure)
    {
        var self = this;
        var join = _.findWhere(query.from, {as: predicate.left.table});
        self.redis.smembers(keyGenerator.indexKey(self.database, join.table, predicate.left.column, predicate.right.value), function (err, members) {
            var parentJoin = _.findWhere(query.from, {as: join.on.right.table});
            var keys = [];
            async.each(members, function (member, callback) {
                keys.push(keyGenerator.referencesKey(self.database, join.table, member, self.database, parentJoin.table, join.on.right.column));
                callback();
            }, function () {
                var key = uniqid();
                self.cachedKeys.push(key);
                self.redis.sunionstore(key, keys);
                closure(key);
            });
        });
    },
    getPredicateKey: function (query, predicate, closure)
    {
        var self = this;
        var from = query.from[0];
        if (from.as == predicate.left.table) {
            if(predicate.operator == "="){
                closure(keyGenerator.indexKey(self.database, from.table, predicate.left.column, predicate.right.value));
            }else{
                self.redis.keys(keyGenerator.indexKey(self.database, from.table, predicate.left.column, "*"), function(err,keys){
                    var toCache = [];
                    async.each(keys, function(key,callback){
                        var value = key.split(":");
                        value = value[4];
                        switch(predicate.operator)
                        {
                            case "<":
                                if(value < predicate.right.value) toCache.push(key);
                                break;
                            case "<=":
                                if(value <= predicate.right.value) toCache.push(key);
                                break;
                            case ">":
                                if(value > predicate.right.value) toCache.push(key);
                                break;
                            case ">=":
                                if(value >= predicate.right.value) toCache.push(key);
                                break;
                            case "REGEXP":
                                var regexp = new RegExp(predicate.right.value.trim(),"i");
                                if(regexp.test(value)) toCache.push(key);
                                break;
                            case "LIKE":
                                var pattern = predicate.right.value.trim();
                                pattern = pattern.substr(0,1) == "%" ? "("+pattern.substr(1) : "^("+pattern;
                                pattern = pattern.substr(-1) == "%" ? pattern.substr(0,pattern.length-1)+")" : pattern+")$";
                                var regexp = new RegExp(pattern,"i");
                                if(regexp.test(value)) toCache.push(key);
                                break;
                            case "!=":
                                if(value != predicate.right.value) toCache.push(key);
                                break;
                        }
                        callback();
                    }, function(){
                        var key = uniqid();
                        self.cachedKeys.push(key);
                        if(toCache.length > 0){
                            self.redis.sunionstore(key,toCache);
                        }else{
                            self.redis.sadd(key,0);
                            self.redis.srem(key,0);
                        }
                        closure(key);
                    });
                });
            }
        } else {
            self.recursivePredicateKey(query, predicate, function (key) {
                closure(key);
            });
        }
    },
    parsePredicates: function (query, predicate, closure)
    {
        var self = this;
        if (predicate != null) {
            var predicates = {};
            if (predicate.left.type == "column_ref") {
                self.getPredicateKey(query, predicate, function (key) {
                    closure(key);
                });
            } else {
                var left, right;
                var after = _.after(2, function () {
                    var key = uniqid();
                    self.cachedKeys.push(key);
                    if (predicate.operator == "OR") {
                        self.redis.sunionstore(key, [left, right]);
                    } else {
                        self.redis.sinterstore(key, [left, right]);
                    }
                    closure(key);
                });
                self.parsePredicates(query, predicate.left, function (key) {
                    left = key;
                    after();
                });
                self.parsePredicates(query, predicate.right, function (key) {
                    right = key;
                    after();
                });
            }
        } else {
            closure(keyGenerator.tableListKey(self.database, query.from[0].table));
        }
    },
    recursiveSelect: function (query, id, finalJoin, joinStack, closure)
    {
        var self = this;
        var join = joinStack.shift();
        if (join.on.left.as != finalJoin.as) {
            var parentJoin = _.findWhere(query.from, {as: join.on.right.table});
            self.redis.hget(keyGenerator.tableHashKey(self.database, parentJoin.table, id), join.on.right.column, function (err, joinId) {
                self.redis.hget(keyGenerator.tableHashKey(self.database, join.table, joinId), join.on.left.column, function (err, value) {
                    if (joinStack.length > 0) {
                        self.recursiveSelect(query, value, finalJoin, joinStack, function (value) {
                            closure(value);
                        });
                    } else {
                        closure(value);
                    }
                });
            });
        }
    },
    parseSelect: function (query, select, id, closure)
    {
        var self = this;
        if (query.from[0].as == select.table) {
            self.redis.hget(keyGenerator.tableHashKey(self.database, query.from[0].table, id), select.column, function (err, value) {
                closure(value);
            });
        } else {
            var join = _.findWhere(query.from, {as: select.table});
            if (join.on.right.table == query.from[0].as) {
                self.redis.hget(keyGenerator.tableHashKey(self.database, query.from[0].table, id), join.on.right.column, function (err, joinId) {
                    self.redis.hget(keyGenerator.tableHashKey(self.database, join.table, joinId), select.column, function (err, value) {
                        closure(value);
                    });
                });
            } else {
                var finalJoin = _.findWhere(query.from, {as: select.table}),
                    joinStack = [finalJoin];
                while (join.on.right.table != query.from[0].as) {
                    join = _.findWhere(query.from, {as: join.on.right.table});
                    joinStack.unshift(join);
                }
                self.recursiveSelect(query, id, finalJoin, joinStack, function (value) {
                    self.redis.hget(keyGenerator.tableHashKey(self.database, finalJoin.table, value), select.column, function (err, value) {
                        closure(value);
                    });
                });
            }
        }
    }
};