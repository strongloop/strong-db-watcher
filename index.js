'use strict';

var assert = require('assert');
var async = require('async');
var debug = require('debug')('strong-db-watcher');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

exports = module.exports = DbWatcher;
exports.Broadcast = Broadcast;

var triggerFunctions = 'CREATE OR REPLACE FUNCTION notify_OLD() ' +
'RETURNS TRIGGER AS ' +
'$BODY$ ' +
'DECLARE ' +
'BEGIN ' +
'  PERFORM pg_notify(TG_TABLE_NAME, ' +
'    TG_WHEN || \' \' || TG_OP || \' \' || TG_TABLE_NAME ' +
'    || \' Old Row:\' || row_to_json(OLD)); ' +
'  RETURN OLD; ' +
'END; ' +
'$BODY$ LANGUAGE plpgsql;' +

'CREATE OR REPLACE FUNCTION notify_NEW() ' +
'RETURNS TRIGGER AS ' +
'$BODY$ ' +
'DECLARE ' +
'BEGIN ' +
'  PERFORM pg_notify(TG_TABLE_NAME, ' +
'    TG_WHEN || \' \' || TG_OP || \' \' || TG_TABLE_NAME ' +
'    || \' New Row:\' || row_to_json(NEW)); ' +
'  RETURN NEW; ' +
'END; ' +
'$BODY$ LANGUAGE plpgsql;';

var triggerCreate = 'BEGIN; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_before_CU_%s ON %s; ' +
'CREATE TRIGGER db_watcher_trig_before_CU_%s ' +
'BEFORE INSERT OR UPDATE ON %s ' +
'FOR EACH ROW EXECUTE PROCEDURE notify_NEW(); ' +
'COMMIT; ' +

'DROP TRIGGER IF EXISTS db_watcher_trig_after_CU_%s ON %s; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_before_D_%s ON %s; ' +

'BEGIN; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_after_D_%s ON %s; ' +
'CREATE TRIGGER db_watcher_trig_after_D_%s ' +
'AFTER DELETE ON %s ' +
'FOR EACH ROW EXECUTE PROCEDURE notify_OLD(); ' +
'COMMIT;';

var triggerDrop = 'BEGIN; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_before_CU_%s ON %s; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_after_CU_%s ON %s; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_before_D_%s ON %s; ' +
'DROP TRIGGER IF EXISTS db_watcher_trig_after_D_%s ON %s; ' +
'COMMIT;';

function DbWatcher(dbClient, changeListener) {
  if (!(this instanceof DbWatcher)) {
    return new DbWatcher(dbClient, changeListener);
  }
  assert(dbClient, 'dbClient is mandatory');
  this._savedClient = updateClient(this, dbClient);
  this._tableSchema = {};
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
  var self = this;
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
        var schema = self._tableSchema[msg.channel];
        if (schema && schema[key].indexOf('timestamp') > -1) {
          value = new Date(value);
          if (value.toString().indexOf('Invalid Date') > -1)
              value = params[key];
        } else {
          value = params[key];
        }
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
  var self = this;
  assert(callback, 'Callback must be set.');
  assert(name, 'Table name must be set.');
  // return if already watching
  if (self._tableSchema === null) self._tableSchema = {};
  if (name in self._tableSchema) return callback();
  debug('watch table: ' + name);
  self._savedClient.query('LISTEN ' + name, function(err) {
    if (err) return callback(err);
    debug('listen table: ' + name);
    var query = 'SELECT column_name, data_type FROM ' +
      'information_schema.columns WHERE table_name=$1;';
    self._savedClient.query(query, [name], function(err, result) {
      var columnInfo = {};
      if (!err) result.rows.forEach(function(prop) {
        columnInfo[prop.column_name] = prop.data_type;
      });
      self._tableSchema[name] = columnInfo;
      Broadcast(self._savedClient, [name], function(err) {
        callback(err);
      });
    });
  });
};

DbWatcher.prototype.isWatching = function(name, fn) {
  return name in this._tableSchema && this.listeners(name).indexOf(fn) >= 0;
};

// TODO
// Test passed, but in some cases, the quries fail.
// Err check removed for now.
DbWatcher.prototype.unwatchTable = function(name, callback) {
  var self = this;
  assert(callback, 'Callback must be set.');
  assert(name, 'Table name must be set.');
  debug('unwatch table: ' + name);
  async.series([
    function(asyncCb) {
      debug('unlisten table: ' + name);
      self._savedClient.query('UNLISTEN ' + name, function() {
        asyncCb(); // ignore err
      });
    },
    function(asyncCb) {
      var statements = triggerDrop.replace(/%s/g, name);
      debug('drop trigger: ' + name);
      self._savedClient.query(statements, function() {
        asyncCb(); // ignore err
      });
    }],
    function(err) {
      self.removeAllListeners(name);
      delete self._tableSchema[name];
      callback(err);
    });
};

DbWatcher.prototype.close = function(callback) {
  assert(callback, 'Callback must be set.');
  if (this._tableSchema === null) {
    setImmediate(callback);
    return;
  }
  var self = this;
  var tableNames = Object.keys(self._tableSchema);
  if (tableNames.length === 0) {
    self._tableSchema = null;
    setImmediate(callback);
    return;
  }
  async.eachSeries(tableNames, self.unwatchTable.bind(self), function(err) {
    if (err) debug('unwatchTable error:' + err);
    self._tableSchema = null;
    debug('closed all tables');
    setImmediate(callback);
    return;
  });
};

function Broadcast(dbClient, tableNames, callback) {
  assert(dbClient, 'dbClient is mandatory');
  assert(util.isArray(tableNames), 'tableNames shoud be an array.');
  assert(callback, 'callback is mandatory');
  if (tableNames.length > 0) setImmediate(function() {
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
