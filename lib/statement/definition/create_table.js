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
        var pattern = new RegExp(/^CREATE TABLE (\w+) (.*)$/, 'i');
        if(matches = queryString.match(pattern)){
            var table = matches[1].trim(),
                definition = matches[2].trim(),
                columns = [],
                types = ["int","decimal","string","date","datetime"],
                error = false;
            redis.sismember(keyGenerator.tablesListKeySchema(database),table, function(err,exists){
                if(!exists){
                    async.each(definition.split(","), function(column,callback){
                        var pattern = /^(int|decimal|string|date|datetime)(.*) (\w+)$/;
                        if(matches = column.match(pattern)){
                            var type = matches[1].trim().toLowerCase(),
                                primary_key = matches[2].trim() != "" ? matches[2].trim().match(/PRIMARY KEY/i) !== null : false,
                                auto_increment = matches[2].trim() != "" ? matches[2].trim().match(/AUTO INCREMENT/i) !== null : false,
                                nullable = matches[2].trim() != "" ? matches[2].trim().match(/NULLABLE/i) !== null : false,
                                name = matches[3].trim();
                            if(_.indexOf(types,type) > -1){
                                if(auto_increment && !primary_key){
                                    error = true;
                                    queryResponse(response.send(queryString,false,{column:column},103));
                                    callback();
                                }else{
                                    if(auto_increment && type != "int"){
                                        queryResponse(response.send(queryString,false,{column:column},106));
                                    }else{
                                        columns.push({type: type,name: name,primary_key: primary_key,auto_increment: auto_increment,nullable:nullable});
                                    }
                                    callback();
                                }
                            }else{
                                queryResponse(response.send(queryString,false,{column:column},108));
                            }

                        }else{
                            error = true;
                            queryResponse(response.send(queryString,false,{column:column},102));
                            callback();
                        }
                    }, function(){
                        if(!error){
                            var primary_key = _.where(columns,{primary_key:true});
                            if(primary_key.length == 0){
                                queryResponse(response.send(queryString,false,{},105));
                            }else if(primary_key.length > 1){
                                queryResponse(response.send(queryString,false,{primary_keys:primary_key},104));
                            }else{
                                if(_.uniq(_.map(columns, function(column){return column.name;})).length == columns.length){
                                    transaction.sadd(keyGenerator.tablesListKeySchema(database), table);
                                    transaction.hmset(keyGenerator.tableHashKeySchema(database, table), {primary_key: primary_key[0].name,auto_increment: primary_key[0].auto_increment ? 1 : 0,name: table,database: database});
                                    transaction.set(keyGenerator.primaryKeySchema(database, table, primary_key[0].name), 0);
                                    async.each(columns, function(column, callback){
                                        transaction.sadd(keyGenerator.columnsListKeySchema(database, table), column.name);
                                        transaction.hmset(keyGenerator.columnHashKeySchema(database, table, column.name), {name: column.name,type: column.type,nullable: column.nullable,table: table,database: database});
                                        callback();
                                    },function(){
                                        queryResponse(response.send(queryString,true,{table:table, columns:columns}));
                                    });
                                }else{
                                    queryResponse(response.send(queryString,false,{table:table, columns:columns},109));
                                }
                            }
                        }
                    });
                }else{
                    queryResponse(response.send(queryString,false,{table:table},110));
                }
            });
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};