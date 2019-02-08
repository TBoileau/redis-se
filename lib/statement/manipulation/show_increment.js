/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    moment = require("moment"),
    async = require("async"),
    uniqid = require('uniqid'),
    multisort = require("multisort"),
    _ = require("underscore"),
    parser = require("node-sqlparser").parse;
module.exports = {
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        var pattern = new RegExp(/^SHOW INCREMENT FROM (\w+)/,"i");
        if (definition = queryString.match(pattern))
        {
            redis.hget(keyGenerator.tableHashKeySchema(database,table),"primary_key", function(err,primary_key){
                client.get(keyGenerator.primaryKeySchema(database, table, primary_key), function(err,auto_increment){
                    queryResponse(response.send(queryString,true,parseInt(auto_increment)+1));
                });
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};