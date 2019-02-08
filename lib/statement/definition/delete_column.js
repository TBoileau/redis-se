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
        var pattern = new RegExp(/^DELETE COLUMN (\w+) FROM (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[2].trim(),
                column = matches[1].trim();

            redis.hget(keyGenerator.tableHashKeySchema(database, table),"primary_key", function(err,primary_key) {
                if (primary_key == column) {
                    queryResponse(response.send(queryString, false, {
                        primary_key: primary_key,
                        column: column
                    }, 113));
                } else {
                    var afterData = _.after(1,function(){
                        transaction.srem(keyGenerator.columnsListKeySchema(database, table), column);
                        transaction.del(keyGenerator.columnHashKeySchema(database, table, column));
                        queryResponse(response.send(queryString,true,{table:table,column:column}));
                    });

                    var afterReferences = _.after(1,function(){
                        redis.smembers(keyGenerator.tableListKey(database, table), function (err, members) {
                            async.each(members, function (member, callback) {
                                transaction.hdel(keyGenerator.tableHashKey(database, table, member), column);
                                callback();
                            }, function(){
                                afterData();
                            });
                        });
                    });

                    var afterIndex = _.after(1,function(){
                        redis.hget(keyGenerator.columnHashKeySchema(database,table,column),"references", function(err,references){
                            if(references == null){
                                afterReferences();
                            }else{
                                delete_constraint.query(transaction,redis,"DELETE CONSTRAINT FOREIGN KEY "+table+"("+column+")",database,function(reply){
                                    if(reply.status){
                                        afterReferences();
                                    }else{
                                        queryResponse(reply);
                                    }
                                });
                            }
                        });
                    });

                    redis.keys(keyGenerator.indexKey(database, table, column, "*"), function(err,keys){
                        async.each(keys,function(key,callback2){
                            transaction.del(key);
                            callback2();
                        }, function(){
                            afterIndex();
                        });
                    });
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};