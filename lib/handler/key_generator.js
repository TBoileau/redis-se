/**
 * Created by toham on 14/11/2016.
 */
module.exports = {
    separator: ":",
    data_key: "data:",
    index_key: "index:",
    cache_key: "cache:",
    view_key: "view:",
    schema_key: "schema:",
    database_list: "schema:databases",
    databaseListKeySchema : function() {
        return this.database_list;
    },
    cacheKey: function(key) {
        return this.cache_key + key;
    },
    databaseHashKeySchema: function (database) {
        return this.schema_key + database;
    },
    tablesListKeySchema: function (database) {
        return this.databaseHashKeySchema(database) + this.separator + "tables";
    },
    tableHashKeySchema: function (database, table) {
        return this.databaseHashKeySchema(database) + this.separator + table;
    },
    columnsListKeySchema: function (database, table) {
        return this.tableHashKeySchema(database, table) + this.separator + "columns";
    },
    columnHashKeySchema: function (database, table, column) {
        return this.tableHashKeySchema(database, table) + this.separator + column;
    },
    primaryKeySchema: function (database, table, column) {
        return this.columnHashKeySchema(database, table, column) + this.separator + "increment";
    },
    referencesListKeySchema: function (database, table, column) {
        return this.columnHashKeySchema(database, table, column) + this.separator + "references";
    },
    tableListKey: function (database, table) {
        return this.data_key + database + this.separator + table + "s";
    },
    tableHashKey: function (database, table, primary_key) {
        return this.data_key + database + this.separator + table + this.separator + primary_key;
    },
    referencesKey: function (database, from_table, primary_key, to_table, to_column) {
        return this.data_key + database + this.separator + from_table + this.separator + primary_key + this.separator + to_table + this.separator + to_column;
    },
    indexKey: function (database, table, column, value) {
        return this.index_key + database + this.separator + table + this.separator + column + this.separator + value;
    }
}