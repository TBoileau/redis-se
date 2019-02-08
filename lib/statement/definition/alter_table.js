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
        var pattern = new RegExp(/^ALTER TABLE (\w+) SET NAME (\w+)$/, 'i');
        if(matches = queryString.match(pattern)){
            var old_table = matches[1].trim(),
                new_table = matches[2].trim();

            redis.exists(keyGenerator.tableHashKeySchema(database,old_table), function(err,exists) {
                if (exists) {
                    var afterData = _.after(1, function () {
                        transaction.srem(keyGenerator.tablesListKeySchema(database), old_table);
                        transaction.sadd(keyGenerator.tablesListKeySchema(database), new_table);
                        transaction.hset(keyGenerator.tableHashKeySchema(database, old_table), "name", new_table);
                        transaction.hget(keyGenerator.tableHashKeySchema(database, old_table), "primary_key", function (err, primary_key) {
                            transaction.renamenx(keyGenerator.tableListKey(database, old_table), keyGenerator.tableListKey(database, new_table), function () {
                                transaction.renamenx(keyGenerator.tableHashKeySchema(database, old_table), keyGenerator.tableHashKeySchema(database, new_table), function () {
                                    transaction.renamenx(keyGenerator.primaryKeySchema(database, old_table, primary_key), keyGenerator.primaryKeySchema(database, new_table, primary_key), function () {
                                        transaction.renamenx(keyGenerator.referencesListKeySchema(database, old_table, primary_key), keyGenerator.referencesListKeySchema(database, new_table, primary_key), function () {
                                            transaction.renamenx(keyGenerator.columnsListKeySchema(database, old_table), keyGenerator.columnsListKeySchema(database, new_table), function () {
                                                redis.keys(keyGenerator.columnHashKeySchema(database, old_table, "*"), function (err, keys) {
                                                    async.each(keys, function (key, callback) {
                                                        transaction.hset(key, "table", new_table);
                                                        transaction.renamenx(key, key.replace(keyGenerator.columnHashKeySchema(database, old_table, ""), keyGenerator.columnHashKeySchema(database, new_table, "")), function () {
                                                            callback();
                                                        });
                                                    }, function () {
                                                        queryResponse(response.send(queryString, true, {table: new_table}));
                                                    });
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });

                    var afterReferences = _.after(1, function () {
                        redis.keys(keyGenerator.tableHashKey(database, old_table, "*"), function (err, keys) {
                            async.each(keys, function (key, callback2) {
                                transaction.renamenx(key, key.replace(keyGenerator.tableHashKey(database, old_table, ""), keyGenerator.tableHashKey(database, new_table, "")));
                                callback2();
                            }, function () {
                                afterData();
                            });
                        });
                    });

                    var afterIndex = _.after(1, function () {
                        redis.keys(keyGenerator.referencesListKeySchema(database, "*", "*", old_table, "*"), function (err, keys) {
                            async.each(keys, function (key, callback2) {
                                transaction.renamenx(key, key.replace(old_table, new_table));
                                callback2();
                            }, function () {
                                afterReferences();
                            });
                        });
                    });

                    redis.keys(keyGenerator.indexKey(database, old_table, "*", "*"), function (err, keys) {
                        async.each(keys, function (key, callback2) {
                            transaction.renamenx(key, key.replace(keyGenerator.indexKey(database, old_table, "", ""), keyGenerator.indexKey(database, new_table, "", "")));
                            callback2();
                        }, function () {
                            afterIndex();
                        });
                    });
                } else {
                    queryResponse(response.send(queryString, false, {}, 107));
                }
            });

        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};