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
        var pattern = new RegExp(/^DELETE FROM (\w+) WHERE (\w+)=(\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[1].trim(),
                primary_key = matches[2].trim(),
                primary_key_value = matches[3].trim(),
                sremKeys = [];
            redis.hgetall(keyGenerator.tableHashKeySchema(database,table), function(err,tb){
                if(tb != null && tb.primary_key == primary_key){
                    var after = _.after(2, function(){
                        transaction.srem(keyGenerator.tableListKey(database, table),primary_key_value);
                        transaction.del(keyGenerator.tableHashKey(database, table, primary_key_value));
                        async.each(sremKeys, function(key,callback){
                            transaction.srem(key,primary_key_value);
                            callback();
                        }, function(){
                            queryResponse(response.send(queryString,true,{}));
                        });
                    });


                    redis.smembers(keyGenerator.referencesListKeySchema(database, table, primary_key), function(err,references){
                        async.each(references, function(reference,callback){
                            redis.hgetall(reference, function(err,ref_definition){
                                redis.smembers(keyGenerator.referencesKey(database, table, primary_key_value, ref_definition.database, ref_definition.table, ref_definition.name), function(err, children){
                                    async.each(children,function(child,callback2){
                                        transaction.hdel(keyGenerator.tableHashKey(ref_definition.database, ref_definition.table, child),ref_definition.name);
                                        callback2();
                                    }, function(){
                                        transaction.del(keyGenerator.indexKey(ref_definition.database, ref_definition.table, ref_definition.name, primary_key_value));
                                        transaction.del(keyGenerator.referencesKey(database, table, primary_key_value, ref_definition.database, ref_definition.table, ref_definition.name));
                                        callback();
                                    }) ;
                                });
                            });
                        }, function(){
                            after();
                        });
                    });

                    redis.hgetall(keyGenerator.tableHashKey(database, table, primary_key_value), function (err, oldValues) {
                        redis.smembers(keyGenerator.columnsListKeySchema(database, table), function (err, table_columns) {
                            async.each(table_columns, function (column, callback) {
                                redis.hexists(keyGenerator.columnHashKeySchema(database, table, column), "references", function (err, referencesExists) {
                                    if (referencesExists) {
                                        redis.hget(keyGenerator.columnHashKeySchema(database, table, column), "references", function (err, reference) {
                                            redis.hgetall(reference, function (err, column_reference) {
                                                redis.sismember(keyGenerator.referencesKey(database, column_reference.table, oldValues[column], database, table, column), primary_key_value, function (err, is_member) {
                                                    if (is_member) {
                                                        transaction.srem(
                                                            keyGenerator.referencesKey(database, column_reference.table, oldValues[column], database, table, column),
                                                            primary_key_value
                                                        );
                                                    }
                                                    sremKeys.push(keyGenerator.indexKey(database, table, column, oldValues[column]));
                                                    callback();
                                                });
                                            });
                                        });
                                    } else {
                                        sremKeys.push(keyGenerator.indexKey(database, table, column, oldValues[column]));
                                        callback();
                                    }
                                });
                            }, function () {
                                after();
                            });
                        });
                    });
                }else{
                    queryResponse(response.send(queryString,false,{},107));
                }
            });

        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};