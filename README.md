redis-se.js v0.1.5
==================

Redis-se.js is a storage engine for Redis. It provide a structured query language to manipulate databases.
Redis is a NoSQL database based on Key/Value system, but with redis-se.js, it's possible to structured databases like relational database management system (MySQL & co). 
This storage engine has been made for manage a lot of structured data. The purpose is the best scalability possible.
I designed it for a project of my agency, who asked to collect and handle lot of structured data.

Changelog V0.1.5
----------------
* Supported schema statements : `get schema`,`show databases`...

Changelog V0.1.4
----------------
* Supported transactions

Changelog V0.1.3
----------------
* Added a chaching system, then supported of `cache select ... into ... for ...` statement

Changelog V0.1.2
----------------
* Supported of `select` statement
* Supported of `truncate` statement
* Supported of `delete` statement
* Supported of `update` statement
* Supported of `insert` statement

Changelog V0.1.1
----------------
* Supported of `alter table` statement
* Supported of `delete constraint foreign key` statement
* Supported of `delete column` statement
* Supported of `alter column` statement
* Supported of `add constraint foreign key` statement
* Supported of `drop table` statement
* Supported of `create table` statement
* Supported of `select database` statement

Changelog V0.1.0
----------------
* Supported of `alter database` statement
* Supported of `drop database` statement
* Supported of `create database` statement
