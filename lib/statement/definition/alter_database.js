/**
 * Created by toham on 14/11/2016.
 */
var async = require('async'),
    keyGenerator = require("../../handler/key_generator.js"),
    response = require("../../handler/response.js"),
    moment = require("moment");
module.exports = {
    query: function(transaction,redis,queryString,queryResponse)
    {
        var pattern = new RegExp(/^ALTER DATABASE (\w+) SET NAME (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var old_name = matches[1].trim(),
                new_name = matches[2].trim();
            redis.sismember(keyGenerator.databaseListKeySchema(),old_name, function(err,exists) {
                if (exists) {
                    redis.sismember(keyGenerator.databaseListKeySchema(),new_name, function(err,exists){
                        if(exists){
                            queryResponse(response.send(queryString,false,{},100));
                        }else{
                            var error = false;
                            redis.smembers(keyGenerator.tablesListKeySchema(old_name), function(err,tables){
                                async.each(tables, function(table,callback){
                                    redis.smembers(keyGenerator.columnsListKeySchema(old_name, table), function(err,columns){
                                        async.each(columns, function(column,callback2){
                                            redis.hget(keyGenerator.columnHashKeySchema(old_name, table, column), "references", function (err, reference) {
                                                if(reference != null){
                                                    transaction.hset(keyGenerator.columnHashKeySchema(old_name, table, column),"references",reference.replace("schema:"+old_name,"schema:"+new_name));
                                                }
                                                transaction.hset(keyGenerator.columnHashKeySchema(old_name, table, column),"database",new_name);
                                                callback2();
                                            });
                                        }, function(){
                                            redis.hget(keyGenerator.tableHashKeySchema(old_name, table),"primary_key", function(err,primary_key){
                                                redis.exists(keyGenerator.referencesListKeySchema(old_name, table, primary_key), function(err,exists){
                                                    if(exists){
                                                        redis.smembers(keyGenerator.referencesListKeySchema(old_name, table, primary_key), function(err,references){
                                                            async.each(references,function(reference,callback3){
                                                                transaction.sadd(keyGenerator.referencesListKeySchema(old_name, table, primary_key),reference.replace("schema:"+old_name,"schema:"+new_name));
                                                                transaction.srem(keyGenerator.referencesListKeySchema(old_name, table, primary_key),reference);
                                                                callback3();
                                                            }, function(){
                                                                transaction.hset(keyGenerator.tableHashKeySchema(old_name, table),"database",new_name);
                                                                callback();
                                                            })
                                                        });
                                                    }else{
                                                        transaction.hset(keyGenerator.tableHashKeySchema(old_name, table),"database",new_name);
                                                        callback();
                                                    }
                                                });
                                            });
                                        });
                                    });
                                }, function(){
                                    async.each(["schema","data","index","view","cache"], function(prefix,callback){
                                        redis.keys(prefix+":"+old_name+"*", function(err,keys){
                                            async.each(keys, function(key,callback2){
                                                var pattern = "^"+prefix+":"+old_name+"(.*)$";
                                                if(matches = key.match(pattern,"i")){
                                                    transaction.rename(key,prefix+":"+new_name+matches[1]);
                                                    callback2();
                                                }else{
                                                    callback2();
                                                }
                                            }, function(){
                                                callback();
                                            });
                                        });
                                    }, function() {
                                        transaction.srem(keyGenerator.databaseListKeySchema(),old_name);
                                        transaction.sadd(keyGenerator.databaseListKeySchema(),new_name);
                                        transaction.hmset(keyGenerator.databaseHashKeySchema(new_name), { name: new_name, updated_at: moment().unix() });
                                        queryResponse(response.send(queryString,true,{old_name:old_name, new_name:new_name}));
                                    });
                                });
                            });
                        }
                    });
                } else {
                    queryResponse(response.send(queryString, false, {}, 101));
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};