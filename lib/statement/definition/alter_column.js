/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    delete_constraint = require("./delete_constraint.js"),
    moment = require("moment"),
    async = require("async"),
    _ = require("underscore");
module.exports = {
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        var pattern = new RegExp(/^ALTER COLUMN (\w+) FROM (\w+) SET (int|decimal|string|date|datetime) (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[2].trim(),
                old_column = matches[1].trim(),
                new_type = matches[3].trim().toLowerCase(),
                new_column = matches[4].trim();
            redis.hget(keyGenerator.tableHashKeySchema(database, table),"primary_key", function(err,primary_key){
                if(primary_key == old_column){
                    queryResponse(response.send(queryString,false,{primary_key:primary_key,column:old_column},113));
                }else{
                    redis.sismember(keyGenerator.columnsListKeySchema(database, table),old_column, function(err,exists){
                        if(exists){
                            redis.sismember(keyGenerator.columnsListKeySchema(database, table),new_column, function(err,exists){
                                if(exists){
                                    queryResponse(response.send(queryString,false,{column:new_column},114));
                                } else{
                                    redis.hgetall(keyGenerator.columnHashKeySchema(database, table, old_column),"type", function(err,old_type){
                                        if(old_column == new_column && old_type == new_type){
                                            queryResponse(response.send(queryString,false,{},112));
                                        }else{
                                            var afterData = _.after(1, function(){
                                                transaction.hmset(keyGenerator.columnHashKeySchema(database,table,old_column),{type:new_type,name:new_column});
                                                transaction.renamenx(keyGenerator.columnHashKeySchema(database,table,old_column),keyGenerator.columnHashKeySchema(database,table,new_column));
                                                transaction.srem(keyGenerator.columnsListKeySchema(database,table),old_column);
                                                transaction.sadd(keyGenerator.columnsListKeySchema(database,table),new_column);
                                                queryResponse(response.send(queryString,true,{old:{name:old_column,type:old_type},new:{name:new_column,type:new_type}}));
                                            });
                                            var afterIndex = _.after(1,function(){
                                                redis.smembers(keyGenerator.tableListKey(database, table), function (err, members) {
                                                    async.each(members, function (member, callback) {
                                                        redis.hget(keyGenerator.tableHashKey(database, table, member), old_column, function(err,value){
                                                            if(old_type != new_type){
                                                                if(!((new_type=="string" && old_type=="integer") || (new_type == "date" && old_type == "datetime") || (old_type == "date" && new_type == "datetime") || (new_type == "integer" && old_type == "datetime") || (old_type == "date" && new_type == "integer"))){
                                                                    value = "null";
                                                                }
                                                            }
                                                            transaction.hdel(keyGenerator.tableHashKey(database, table, member), old_column);
                                                            transaction.hset(keyGenerator.tableHashKey(database, table, member), new_column,value);
                                                        });
                                                        callback();
                                                    }, function(){
                                                        afterData();
                                                    });
                                                });
                                            });
                                            redis.keys(keyGenerator.indexKey(database, table, old_column, "*"), function(err,keys){
                                                async.each(keys,function(key,callback2){
                                                    var new_key = key;
                                                    if(old_column != new_column){
                                                        new_key = new_key.replace(old_name,new_name);
                                                    }
                                                    if(old_type != new_type){
                                                        if(!((new_type=="string" && old_type=="integer") || (new_type == "date" && old_type == "datetime") || (old_type == "date" && new_type == "datetime") || (new_type == "integer" && old_type == "datetime") || (old_type == "date" && new_type == "integer"))){
                                                            new_key = keyGenerator.indexKey(database,table,new_column,"null");
                                                        }
                                                    }
                                                    transaction.renamenx(key,new_key);
                                                    callback2();
                                                }, function(){
                                                    afterIndex();
                                                });
                                            });
                                        }
                                    });
                                }
                            });
                        } else{
                            queryResponse(response.send(queryString,false,{column:old_column},111));
                        }
                    });
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};