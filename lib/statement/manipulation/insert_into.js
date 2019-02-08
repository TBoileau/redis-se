/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    moment = require("moment"),
    async = require("async"),
    _ = require("underscore"),
    parser = require("node-sqlparser").parse;
module.exports = {
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        try{
            var query = parser(queryString);
            redis.exists(keyGenerator.tableHashKeySchema(database,query.table), function(err,exists){
                if (exists) {
                    redis.hgetall(keyGenerator.tableHashKeySchema(database,query.table), function(err,table){
                        if(!table.auto_increment && _.indexOf(query.columns,table.primary_key) == -1){
                            queryResponse(response.send(queryString, false, {}, 115));
                        }else{
                            redis.smembers(keyGenerator.columnsListKeySchema(database,query.table),function(err,members){
                                var error = false;
                                var notnullcolumns = [];
                                var errortypecolumns = [];
                                if(_.difference(query.columns,members) > 0){
                                    queryResponse(response.send(queryString, false, _.difference(query.columns,members), 119));
                                }else{
                                    async.each(members, function(member,callback){
                                        redis.hgetall(keyGenerator.columnHashKeySchema(database,query.table,member), function(err,column){
                                            if(!column.nullable && _.indexOf(query.columns,column.name)){
                                                notnullcolumns.push(column.name);
                                                error = true;
                                                callback();
                                            }else{
                                                var types = _.filter(query.values,function(t){ return t.value[_.indexOf(query.columns,column.name)].type == "number"; });
                                                if(types.length != query.values.length && _.indexOf(["date","datetime","decimal","int"],column.type) > -1){
                                                    errortypecolumns.push(column);
                                                    error = true;
                                                }
                                                callback();
                                            }
                                        });
                                    }, function(){
                                        if(error){
                                            if(notnullcolumns.length > 0 && errortypecolumns.length > 0){
                                                queryResponse(response.send(queryString, false, {nullable: notnullcolumns, types: errortypecolumns}, 118));
                                            }else if(notnullcolumns.length > 0){
                                                queryResponse(response.send(queryString, false, notnullcolumns, 116));
                                            }else{
                                                queryResponse(response.send(queryString, false, errortypecolumns, 117));
                                            }
                                        }else{
                                            var pkValues = [];
                                            async.each(query.values, function(value,callback){
                                                var pkValue;
                                                var afterPk = _.after(1, function(){
                                                    pkValues.push(pkValue);
                                                    transaction.sadd(keyGenerator.tableListKey(database,query.table),pkValue);
                                                    transaction.hset(keyGenerator.tableHashKey(database,query.table,pkValue),table.primary_key,pkValue);
                                                    transaction.sadd(keyGenerator.indexKey(database, query.table, table.primary_key, pkValue), pkValue);
                                                    async.forEachOf(query.columns, function(column,index,callback2){
                                                        if(column != table.primary_key){
                                                            var new_value = value.value[index].value;
                                                            transaction.hset(keyGenerator.tableHashKey(database,query.table,pkValue),column,new_value);
                                                            transaction.sadd(keyGenerator.indexKey(database, query.table, column,new_value), pkValue);
                                                            redis.hexists(keyGenerator.columnHashKeySchema(database, query.table, column), "references", function (err, referencesExists) {
                                                                if (referencesExists) {
                                                                    redis.hget(keyGenerator.columnHashKeySchema(database, query.table, column), "references", function (err, reference) {
                                                                        redis.hgetall(reference, function (err, column_reference) {
                                                                            transaction.sadd(keyGenerator.referencesKey(column_reference.database, column_reference.table, new_value, query.table, column), pkValue);
                                                                            callback2();
                                                                        });
                                                                    });
                                                                }else{
                                                                    callback2();
                                                                }
                                                            });
                                                        }else{
                                                            callback2();
                                                        }
                                                    }, function(){
                                                        callback();
                                                    });
                                                });
                                                if(table.auto_increment){
                                                    redis.incr(keyGenerator.primaryKeySchema(database,query.table,table.primary_key), function(err,id){
                                                        pkValue = id;
                                                        afterPk();
                                                    });
                                                }else{
                                                    pkValue = value.value[_.indexOf(query.columns,table.primary_key)].value;
                                                    afterPk();
                                                }
                                            }, function(){
                                                queryResponse(response.send(queryString, true, pkValues));
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                } else {
                    queryResponse(response.send(queryString, false, {}, 107));
                }
            });
        }catch(exception){
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};