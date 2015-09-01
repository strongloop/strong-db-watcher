'use strict';

var assert = require('assert');
var async = require('async');
var debug = require('debug')('strong-db-watcher');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var util = require('util');

exports = module.exports = DbWatcher;
exports.Broadcast = Broadcast;

function DbWatcher(dbClient, changeListener) {
  if (!(this instanceof DbWatcher)) {
    return new DbWatcher(dbClient, changeListener);
  }
  assert(dbClient, 'dbClient is mandatory');
  this._savedClient = updateClient(this, dbClient);
  this._tableNames = [];
  if (changeListener) this.on('change', changeListener);
}
util.inherits(DbWatcher, EventEmitter);

function updateClient(ctxt, dbClient) {
  if (dbClient !== ctxt._savedClient) {
    dbClient.on('notification', processMessage.bind(ctxt));
    ctxt._savedClient = dbClient;
  }
  return ctxt._savedClient;
}

/* msg is {channel: <string>, payload: <string>} where
 *    channel: tableName,
 *    payload: <when> + <op> + <tableName> + <record>
 * where <when>, <op>, <tableName> and <record> are strings.
 *
 * processMessage parses the record string into an object.
 * Record is a serialized string of database record which
 * is defined by the table schema.
 * Record field can be a stringified oject.
 *
 * see notify_OLD() and notify_NEW() in trigger-functionss.txt,
 * trigger arguments such as TG_WHEN, TG_OP, TG_TABLE_NAME are
 * described in:
 * http://www.postgresql.org/docs/9.2/static/plpgsql-trigger.html
 */

function processMessage(msg) {
  var pos = msg.payload.indexOf('Row:') + 'Row:'.length;
  var metaStr = msg.payload.substring(0, pos);
  var metaParams = metaStr.split(' ');
  // trim ( and ) and wrap with { }
  var rowStr = '{' + msg.payload.substring(pos + 1,
      msg.payload.length - 1) + '}';
  var params = JSON.parse(rowStr);
  Object.keys(params).forEach(function(key) {
    var value = params[key];
    if (typeof value === 'string') {
      try {
        value = JSON.parse(value);
      } catch (e) {
        value = new Date(value);
        if (value.toString().indexOf('Invalid Date') > -1) value = params[key];
      }
      params[key] = value;
    }
  });
  var resObj = {
    table: msg.channel,
    op: metaParams[1],
    when: metaParams[0],
    payload: params,
    receivedAt: Date.now()};
  this.emit(resObj.table, resObj);
  this.emit('change', resObj);
}

DbWatcher.prototype.watchTable = function(name, callback) {
  assert(callback, 'Callback must be set.');
  assert(name, 'Table name must be set.');
  var self = this;
  self._savedClient.query('LISTEN ' + name, function(err) {
    if (err) return callback(err);
    debug('Listening to %s ...', name);
    self._tableNames.push(name);
    callback();
  });
};

DbWatcher.prototype.unwatchTable = function(name, callback) {
  assert(callback, 'Callback must be set.');
  assert(name, 'Table name must be set.');
  this._savedClient.query('UNLISTEN ' + name, callback);
};

DbWatcher.prototype.close = function(callback) {
  assert(callback, 'Callback must be set.');
  var self = this;
  var unsubFailure = null;
  this._tableNames.forEach(function(name, index) {
    self.unwatchTable(name, function(err) {
      unsubFailure = unsubFailure || err;
      if (this === self._tableNames.length - 1) {
        self._tableNames = null;
        callback(unsubFailure);
      }
    }.bind(index));
  });
};

function Broadcast(dbClient, tableNames, callback) {
  assert(dbClient, 'dbClient is mandatory');
  assert(util.isArray(tableNames), 'tableNames shoud be an array.');
  assert(callback, 'callback is mandatory');
  if (tableNames.length > 0) setImmediate(function() {
    var triggerFunctions = fs.readFileSync(
        __dirname + '/trigger-functions.txt', 'utf8');
    var triggerCreate = fs.readFileSync(
        __dirname + '/trigger-create.txt', 'utf8');

    async.series([
      function(asyncCb) {
        dbClient.query(triggerFunctions, asyncCb);
      },
      function(asyncCb) {
        async.each(tableNames, function(name, tableCb) {
          var statements = triggerCreate.replace(/%s/g, name);
          dbClient.query(statements, tableCb);
        }, asyncCb);
      }], callback);
  });
}
