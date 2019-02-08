/**
 * Created by toham on 14/11/2016.
 */
var response = require("../../handler/response.js"),
    keyGenerator = require("../../handler/key_generator.js"),
    select = require("./select.js");
module.exports = {
    cachedKeys: [],
    redis: null,
    database: null,
    query: function(transaction,redis,queryString,database,queryResponse)
    {
        cache_pattern = new RegExp(/CACHE (.*) INTO (\w*) FOR (\d*)/, 'i');
        if(matches = queryString.match(cache_pattern)){
            var keyStore = matches[2],
                time = matches[3];
            redis.exists(keyGenerator.cacheKey(keyStore), function(err,exist){
                if(exist){
                    redis.get(keyGenerator.cacheKey(keyStore), function(err,cacheResults){
                        queryResponse(response.send(queryString,true,JSON.parse(cacheResults)));
                    });
                }else{
                    select.query(redis,matches[1],database,function(reply){
                        if(reply.status){
                            transaction.set(keyGenerator.cacheKey(keyStore),JSON.stringify(reply.data));
                            transaction.expire(keyGenerator.cacheKey(keyStore),time);
                            queryResponse(reply);
                        } else{
                            queryResponse(reply);
                        }
                    });
                }
            })
        }else{
            queryResponse(response.send(queryString,false,{},2));
        }
    }
};