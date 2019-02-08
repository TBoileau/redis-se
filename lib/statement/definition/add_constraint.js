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
        var pattern = new RegExp(/^ADD CONSTRAINT FOREIGN KEY (\w+)\((\w+)\) REFERENCES (\w+)\((\w+)\)$/, 'i');
        if(matches = queryString.match(pattern)){
            var from_table = matches[1].trim(),
                from_column = matches[2].trim(),
                to_table = matches[3].trim(),
                to_column = matches[4].trim();
            redis.sismember(keyGenerator.tablesListKeySchema(database),from_table, function(err,exists){
                if(exists){
                    redis.sismember(keyGenerator.columnsListKeySchema(database,from_table),from_column, function(err,exists){
                        if(exists){
                            redis.sismember(keyGenerator.tablesListKeySchema(database),to_table, function(err,exists){
                                if(exists){
                                    redis.sismember(keyGenerator.columnsListKeySchema(database,to_table),to_column, function(err,exists){
                                        if(exists){
                                            transaction.hset(keyGenerator.columnHashKeySchema(database, from_table, from_column), "references", keyGenerator.columnHashKeySchema(database, to_table, to_column));
                                            transaction.sadd(keyGenerator.referencesListKeySchema(database, to_table, to_column), keyGenerator.columnHashKeySchema(database, from_table, from_column));
                                            queryResponse(response.send(queryString,true,{from:{table:from_table,column:from_column},to:{table:to_table,column:to_column}},111));
                                        }else{
                                            queryResponse(response.send(queryString,false,{column:to_column},111));
                                        }
                                    });
                                }else{
                                    queryResponse(response.send(queryString,false,{table:to_table},107));
                                }
                            });
                        }else{
                            queryResponse(response.send(queryString,false,{column:from_column},111));
                        }
                    });
                    
                }else{
                    queryResponse(response.send(queryString,false,{table:from_table},107));
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};