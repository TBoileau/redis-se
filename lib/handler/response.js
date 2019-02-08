/**
 * Created by toham on 14/11/2016.
 */
var moment = require('moment');
module.exports = {
    start: function(){
        this.startTime = moment().unix();
    },
    startTime: null,
    errors: {
        0 : "Server is not running",
        1 : "This query doesn't match with any SQL statements",
        2 : "This is not a valid query",
        3 : "An error occured during execution",
        4 : "An error occured during the recovery of an error",
        5 : "The password is not valid",
        100 : "This database already exists",
        101 : "This database does not exists",
        102 : "The column definition is not valid",
        103 : "Auto increment column but not primary key is not possible",
        104 : "Only one primary key in table",
        105 : "Your table must have contain one primary key",
        106 : "Auto increment column must have in int",
        107 : "This table does not exist",
        108 : "This type is not valid",
        109 : "Some columns are same name, it's not possible",
        110 : "This table already exists",
        111 : "This column does not exist",
        112 : "Nothing to alter",
        113 : "Alter or delete primary key is not possible",
        114 : "This column already exists",
        115 : "Primary key is not auto increment and it's missing in query",
        116 : "This column(s) can not be null",
        117 : "The type of value(s) is not valid",
        118 : "Some columns can not be null, types of some values are not valid",
        119 : "Some columns are not recognized",
        120 : "Some rows did not be removed",
        121 : "You must have set the table (or alias) in select statement"
    },
    send: function(query,status,data,errno)
    {
        if(status){
            return {
                query: query,
                data: data,
                status: 1,
                time: moment().unix()-this.startTime
            };
        }else{
            return {
                query: query,
                status: 0,
                data: data,
                error: {
                    code: errno,
                    message: this.errors[errno]
                },
                time: moment().unix()-this.startTime
            }
        }
    }
}