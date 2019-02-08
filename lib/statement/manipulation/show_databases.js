/**
 * Created by toham on 14/11/2016.
 */
var async = require('async'),
    keyGenerator = require("../../handler/key_generator.js"),
    response = require("../../handler/response.js");
module.exports = {
    query: function(transaction,redis,queryString,queryResponse)
    {
        var pattern = new RegExp(/^SHOW DATABASES$/, 'i');
        if(matches = queryString.match(pattern)){
            redis.smembers(keyGenerator.databaseListKeySchema(), function(err,databases) {
                queryResponse(response.send(queryString,true,{databases:databases}));
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};