/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    moment = require("moment");
module.exports = {
    query: function(transaction,redis,queryString,queryResponse)
    {
        var pattern = new RegExp(/^CREATE DATABASE (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var database = matches[1].trim();
            redis.sismember(keyGenerator.databaseListKeySchema(),database, function(err,exists){
                if(exists){
                    queryResponse(response.send(queryString,false,{},100));
                }else{
                    transaction.sadd(keyGenerator.databaseListKeySchema(),database);
                    transaction.hmset(keyGenerator.databaseHashKeySchema(database), { name: database, created_at: moment().unix() });
                    queryResponse(response.send(queryString,true,{database:database}));
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};