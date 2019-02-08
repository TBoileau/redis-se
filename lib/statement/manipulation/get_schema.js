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
        var pattern = new RegExp(/^GET SCHEMA$/,"i");
        if (queryString.match(pattern))
        {
            var tablesDefinitions = {};
            redis.smembers(keyGenerator.tablesListKeySchema(database), function(err,tables){
                async.each(tables, function(tableName, callback1){
                    redis.hgetall(keyGenerator.tableHashKeySchema(database, tableName), function(err,info){
                        var table = info;
                        table.columns = {};
                        redis.smembers(keyGenerator.columnsListKeySchema(database, tableName), function(err,columns){
                            async.each(columns, function(column, callback2){
                                redis.hgetall(keyGenerator.columnHashKeySchema(database, tableName, column), function(err,property){
                                    if(!_.isUndefined(property.references)){
                                        redis.hgetall(property.references, function(err,references){
                                            property.references = references;
                                            table.columns[column] = property;
                                            callback2();
                                        });
                                    }else{
                                        table.columns[column] = property;
                                        callback2();
                                    }
                                });
                            }, function(){
                                tablesDefinitions[tableName] = table;
                                callback1();
                            });
                        });
                    })
                }, function(){
                    queryResponse(response.send(queryString,true,tablesDefinitions));
                });
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};