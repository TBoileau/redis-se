/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    deleteFrom = require("./delete.js"),
    moment = require("moment"),
    async = require("async"),
    _ = require("underscore"),
    parser = require("node-sqlparser").parse;
module.exports = {
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        var pattern = new RegExp(/^TRUNCATE (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[1].trim();

            redis.hgetall(keyGenerator.tableHashKeySchema(database,table), function(err,tb){
                redis.smembers(keyGenerator.tableListKey(database,table), function(err,members){
                    var error = false;
                    var idErrors = [];
                    async.each(members,function(member,callback){
                        deleteFrom.query(transaction,redis,"DELETE FROM "+table+" WHERE "+tb.primary_key+"="+member,database,function(reply){
                            if(!reply.status){
                                error = true;
                                idErrors.push(member);
                            }
                            callback();
                        });
                    },function(){
                        if(error){
                            queryResponse(response.send(queryString,false,idErrors,120));
                        }else{
                            queryResponse(response.send(queryString,true,{}));
                        }
                    });
                });
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};