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
        var pattern = new RegExp(/^DELETE CONSTRAINT FOREIGN KEY (\w+)\((\w+)\)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[1].trim(),
                column = matches[2].trim();
            redis.sismember(keyGenerator.tablesListKeySchema(database),table, function(err,exists){
                if(exists){
                    redis.sismember(keyGenerator.columnsListKeySchema(database,table),column, function(err,exists){
                        if(exists){
                            redis.hget(keyGenerator.columnHashKeySchema(database, table, column), "references", function(err,reference){
                                redis.hgetall(reference, function (err, column_reference) {
                                    redis.keys(keyGenerator.referencesKey(column_reference.database, column_reference.table, "*", database, table, column), function(err,keys){
                                        async.each(keys,function(key,callback){
                                            transaction.del(key);
                                            callback();
                                        }, function(){
                                            transaction.srem(reference+":references",keyGenerator.columnHashKeySchema(database, table, column));
                                            transaction.hdel(keyGenerator.columnHashKeySchema(database, table, column), "references");
                                            queryResponse(response.send(queryString,true,{table:table,column:column}));
                                        });
                                    });
                                });
                            });
                        }else{
                            queryResponse(response.send(queryString,false,{column:column},111));
                        }
                    });
                }else{
                    queryResponse(response.send(queryString,false,{table:table},107));
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};