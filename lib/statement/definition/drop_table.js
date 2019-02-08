/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    moment = require("moment"),
    async = require("async"),
    _ = require("underscore");
module.exports = {
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        var pattern = new RegExp(/^DROP TABLE (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[1].trim();
            redis.exists(keyGenerator.tableHashKeySchema(database,table), function(err,exists){
                if(exists){
                    // Schema
                    var afterReferences = _.after(1, function(){
                        redis.smembers(keyGenerator.columnsListKeySchema(database, table), function (err, columns) {
                            async.each(columns, function (column,callback) {
                                transaction.srem(keyGenerator.columnsListKeySchema(database, table), column);
                                transaction.del(keyGenerator.columnHashKeySchema(database, table, column));
                                callback();
                            }, function(){
                                redis.hget(keyGenerator.tableHashKeySchema(database,table),"primary_key", function(err,primary_key){
                                    transaction.del(keyGenerator.primaryKeySchema(database,table,primary_key));
                                    transaction.del(keyGenerator.referencesListKeySchema(database, table, primary_key));
                                    transaction.del(keyGenerator.tableHashKeySchema(database,table));
                                    transaction.srem(keyGenerator.tablesListKeySchema(database), table);
                                    queryResponse(response.send(queryString,true,{}));
                                });
                            });
                        });
                    });

                    // References
                    var afterData = _.after(1, function(){
                        redis.smembers(keyGenerator.columnsListKeySchema(database, table), function (err, columns) {
                            async.each(columns, function (column, callback) {
                                redis.hexists(keyGenerator.columnHashKeySchema(database, table, column), "references", function (err, referencesExists) {
                                    if (referencesExists) {
                                        redis.hget(keyGenerator.columnHashKeySchema(database, table, column), "references", function (err, reference) {
                                            redis.hgetall(reference, function (err, column_reference) {
                                                redis.keys(referencesKey(column_reference.database, column_reference.table, "*", database, table, column), function(err,keys){
                                                    async.each(keys,function(key,callback2){
                                                        transaction.del(key);
                                                        callback2();
                                                    }, function(){
                                                        callback();
                                                    });
                                                });
                                            });
                                        });
                                    }else{
                                        callback();
                                    }
                                });
                            }, function(){
                                redis.hget(keyGenerator.tableHashKeySchema(database,table),"primary_key", function(err,primary_key){
                                    redis.exists(keyGenerator.referencesListKeySchema(database, table, primary_key), function(err,exists){
                                        if(exists){
                                            redis.smembers(keyGenerator.referencesListKeySchema(database, table, primary_key), function(err,references){
                                                async.each(references, function(reference,callback){
                                                    redis.hgetall(reference, function(err,ref_definition){
                                                        redis.keys(keyGenerator.referencesKey(database, table, "*", ref_definition.database, ref_definition.table, ref_definition.name), function(err,keys){
                                                            async.each(keys,function(key,callback2){
                                                                redis.smembers(key, function(err, children){
                                                                    async.each(children,function(child,callback3){
                                                                        transaction.hdel(keyGenerator.tableHashKey(ref_definition.database, ref_definition.table, child),ref_definition.name);
                                                                        callback3();
                                                                    }, function(){
                                                                        redis.keys(keyGenerator.indexKey(ref_definition.database, ref_definition.table, ref_definition.name, "*"), function(err,subkeys){
                                                                           async.each(subkeys,function(subkey,callback4){
                                                                               transaction.del(subkey);
                                                                               callback4();
                                                                           }, function(){
                                                                               transaction.del(key);
                                                                               callback2();
                                                                           });
                                                                        });
                                                                    }) ;
                                                                });
                                                            }, function(){
                                                                callback();
                                                            });
                                                        });
                                                    });
                                                }, function(){
                                                    afterReferences();
                                                });
                                            });
                                        }else{
                                            afterReferences();
                                        }
                                    });
                                });
                            });
                        });
                    });

                    // Data
                    var afterIndex = _.after(1, function(){
                        redis.smembers(keyGenerator.tableListKey(database, table), function(err,members){
                            async.each(members,function(member,callback){
                                transaction.del(keyGenerator.tableHashKey(database, table, member));
                                transaction.srem(keyGenerator.tableListKey(database, table),member);
                                callback();
                            }, function(){
                                afterData();
                            });
                        });
                    });

                    // Index
                    redis.smembers(keyGenerator.columnsListKeySchema(database, table), function (err, columns) {
                        async.each(columns, function (column,callback) {
                            redis.keys(keyGenerator.indexKey(database, table, column, "*"), function(err,keys){
                                async.each(keys,function(key,callback2){
                                    transaction.del(key);
                                    callback2();
                                }, function(){
                                    callback();
                                })
                            });
                        }, function(){
                            afterIndex();
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