/**
 * Created by toham on 14/11/2016.
 */
var express = require('express'),
    app = express(),
    router = express.Router(),
    server = require('http').Server(app),
    io = require('socket.io')(server),
    yaml = require('js-yaml'),
    async = require('async'),
    fs = require('fs'),
    redis = require('redis'),
    response = require("./lib/handler/response.js"),
    bodyParser = require("body-parser"),
    config = yaml.load(fs.readFileSync("config.yml", 'utf8')),
    statement = null,
    statements = {
        select_database: {
            handler: "./lib/statement/manipulation/select_database.js",
            pattern: /^SELECT DATABASE(.*)$/,
            mustHaveSelectedDb: false
        },
        create_database: {
            handler: "./lib/statement/definition/create_database.js",
            pattern: /^CREATE DATABASE(.*)$/,
            mustHaveSelectedDb: false
        },
        alter_database: {
            handler: "./lib/statement/definition/alter_database.js",
            pattern: /^ALTER DATABASE(.*)$/,
            mustHaveSelectedDb: false
        },
        drop_database: {
            handler: "./lib/statement/definition/drop_database.js",
            pattern: /^DROP DATABASE(.*)$/,
            mustHaveSelectedDb: false
        },
        create_table: {
            handler: "./lib/statement/definition/create_table.js",
            pattern: /^CREATE TABLE(.*)$/,
            mustHaveSelectedDb: true
        },
        drop_table: {
            handler: "./lib/statement/definition/drop_table.js",
            pattern: /^DROP TABLE(.*)$/,
            mustHaveSelectedDb: true
        },
        add_constraint_table: {
            handler: "./lib/statement/definition/add_constraint.js",
            pattern: /^ADD CONSTRAINT FOREIGN KEY(.*)$/,
            mustHaveSelectedDb: true
        },
        delete_constraint_table: {
            handler: "./lib/statement/definition/delete_constraint.js",
            pattern: /^DELETE CONSTRAINT FOREIGN KEY(.*)$/,
            mustHaveSelectedDb: true
        },
        delete_column: {
            handler: "./lib/statement/definition/delete_column.js",
            pattern: /^DELETE COLUMN(.*)$/,
            mustHaveSelectedDb: true
        },
        alter_column: {
            handler: "./lib/statement/definition/alter_column.js",
            pattern: /^ALTER COLUMN(.*)$/,
            mustHaveSelectedDb: true
        },
        alter_table: {
            handler: "./lib/statement/definition/alter_table.js",
            pattern: /^ALTER TABLE(.*)$/,
            mustHaveSelectedDb: true
        },
        insert_into: {
            handler: "./lib/statement/manipulation/insert_into.js",
            pattern: /^INSERT INTO(.*)$/,
            mustHaveSelectedDb: true
        },
        update: {
            handler: "./lib/statement/manipulation/update.js",
            pattern: /^UPDATE(.*)$/,
            mustHaveSelectedDb: true
        },
        delete: {
            handler: "./lib/statement/manipulation/delete.js",
            pattern: /^DELETE FROM(.*)$/,
            mustHaveSelectedDb: true
        },
        truncate: {
            handler: "./lib/statement/manipulation/truncate.js",
            pattern: /^TRUNCATE(.*)$/,
            mustHaveSelectedDb: true
        },
        select: {
            handler: "./lib/statement/manipulation/select.js",
            pattern: /^SELECT(.*)FROM(.*)$/,
            mustHaveSelectedDb: true
        },
        cache: {
            handler: "./lib/statement/manipulation/caching.js",
            pattern: /^CACHE(.*)INTO(.*)FOR(.*)$/,
            mustHaveSelectedDb: true
        },
        show_databases: {
            handler: "./lib/statement/manipulation/show_databases.js",
            pattern: /^SHOW DATABASES$/,
            mustHaveSelectedDb: false
        },
        get_schema: {
            handler: "./lib/statement/manipulation/get_schema.js",
            pattern: /^GET SCHEMA$/,
            mustHaveSelectedDb: true
        },
        show_increment: {
            handler: "./lib/statement/manipulation/show_increment.js",
            pattern: /^SHOW INCREMENT(.*)$/,
            mustHaveSelectedDb: true
        },
    };

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(router);

router.route('/query').post(function(req,res){
    client = redis.createClient(config.redis.port);
    client.auth(req.body.password);
    var transactions = client.multi();
    client.on("error", function(err){
        transactions.discard();
        res.json(response.send("",false,{},0));
    });

    response.start();
    async.forEachOf(statements,function(value,key,callback){
        if(req.body.query.match(new RegExp(value.pattern,"i")) && ((value.mustHaveSelectedDb && req.body.database !== null) || !value.mustHaveSelectedDb)){
            statement = key;
        }
        callback();
    }, function(){
        if(statement === null){
            res.json(response.send(req.body.query,false,{},1));
        }else{
            try{
                if(statements[statement].mustHaveSelectedDb){
                    require(statements[statement].handler).query(transactions,client,req.body.query,req.body.database,function(stmtReply){
                        transactions.exec(function(err,reply){
                            if(err){
                                res.json(response.send("",false,{},3));
                                io.sockets.emit('monitoring', { query : req.body.query, time : stmtReply.time });
                                client.quit();
                            }else{
                                res.json(stmtReply);
                                io.sockets.emit('monitoring', { query : req.body.query, time : stmtReply.time });
                                client.quit();
                            }
                        });
                    });
                }else{
                    require(statements[statement].handler).query(transactions,client,req.body.query,function(stmtReply){
                        transactions.exec(function(err,reply){
                            if(err){
                                res.json(response.send("",false,{},3));
                                io.sockets.emit('monitoring', { query : req.body.query, time : stmtReply.time });
                                client.quit();
                            }else{
                                res.json(stmtReply);
                                io.sockets.emit('monitoring', { query : req.body.query, time : stmtReply.time });
                                client.quit();
                            }
                        });
                    });
                }
            }catch(e){
                transactions.discard();
                res.json(response.send(req.body.query,false,{},3));
                client.quit();
            }
        }
    });
});

router.route('/connection').post(function(req,res){
    client = redis.createClient(config.redis.port);
    client.on("error", function(){
        res.json(response.send("",false,{},0));
    })
    client.auth(req.body.password, function(err,reply){
        if(reply){
            res.json(response.send("",true,{}));
        }else{
            res.json(response.send("",false,{},5));
        }
    });
});

server.listen(3000, "127.0.0.1");
