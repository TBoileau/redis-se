/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    moment = require("moment"),
    async = require("async"),
    _ = require("underscore"),
    parser = require("node-sqlparser").parse;
module.exports = {
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        try{
            var query = parser(queryString);
            redis.hgetall(keyGenerator.tableHashKeySchema(database,query.table), function(err,table){
                if (table != null) {
                    if(query.where.type == "binary_expr" && query.where.operator == "=" && query.where.left.type == "column_ref" && query.where.left.column == table.primary_key && _.indexOf(_.pluck(query.set,"column"),table.primary_key) == -1){
                        redis.hgetall(keyGenerator.tableHashKey(database, query.table, query.where.right.value), function (err, oldValues) {
                            var error = false;
                            var errortypecolumns = [];
                            async.each(query.set, function (s, callback1) {
                                redis.hgetall(keyGenerator.columnHashKeySchema(database, query.table, s.column), function (err, column) {
                                    if (_.indexOf(["date", "datetime", "decimal", "int"], column.type) == -1 && s.value.type == "number") {
                                        errortypecolumns.push(column);
                                        error = true;
                                    }
                                    callback1();
                                });
                            }, function(){
                                if(error){
                                    queryResponse(response.send(queryString,false,errortypecolumns,117));
                                }else{
                                    async.each(query.set, function (s, callback1) {
                                        transaction.srem(keyGenerator.indexKey(database, query.table, s.column, oldValues[s.column]), query.where.right.value);
                                        transaction.hset(keyGenerator.tableHashKey(database, query.table, query.where.right.value), s.column, s.value.value);
                                        transaction.sadd(keyGenerator.indexKey(database, query.table, s.column, s.value.value), query.where.right.value);
                                        redis.hexists(keyGenerator.columnHashKeySchema(database, query.table, s.column), "references", function (err, referencesExists) {
                                            if (referencesExists) {
                                                redis.hget(keyGenerator.columnHashKeySchema(database, query.table, s.column), "references", function (err, reference) {
                                                    redis.hgetall(reference, function (err, column_reference) {
                                                        redis.sismember(keyGenerator.referencesKey(database, column_reference.table, s.value.value, query.table, s.column), query.where.right.value, function (err, is_member) {
                                                            if (is_member) {
                                                                transaction.smove(
                                                                    keyGenerator.referencesKey(database, column_reference.table, oldValues[s.column], query.table, s.column),
                                                                    keyGenerator.referencesKey(database, column_reference.table, s.value.value, database, query.table, s.column),
                                                                    query.where.right.value
                                                                );
                                                                callback1();
                                                            } else {
                                                                transaction.sadd(keyGenerator.referencesKey(database, column_reference.table, s.value.value, query.table, s.column), query.where.right.value);
                                                                callback1();
                                                            }
                                                        });
                                                    });
                                                });
                                            } else {
                                                callback1();
                                            }
                                        });
                                    },function(){
                                        queryResponse(response.send(queryString,true,{}));
                                    });
                                }
                            });
                        });
                    }else{
                        queryResponse(response.send(queryString,false,{},2));
                    }
                } else {
                    queryResponse(response.send(queryString, false, {}, 107));
                }
            });
        }catch(exception){
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};