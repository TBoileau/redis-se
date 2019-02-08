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
        var pattern = new RegExp(/^DROP DATABASE (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var database = matches[1].trim();
            redis.sismember(keyGenerator.databaseListKeySchema(),database, function(err,exists) {
                if (exists) {
                    var error = false;
                    async.each(["schema","data","index","view","cache"], function(prefix,callback){
                        redis.keys(prefix+":"+database+"*", function(err,keys){
                            async.each(keys, function(key,callback2){
                                transaction.del(key);
                                callback2();
                            }, function(){
                                callback();
                            });
                        });
                    }, function(){
                        if(error){
                            queryResponse(response.send(queryString,false,{},3));
                        }else{
                            transaction.srem(keyGenerator.databaseListKeySchema(),database);
                            queryResponse(response.send(queryString,true,{database:database}));
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