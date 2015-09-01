'use strict';

var maybeSkip = process.env.POSTGRESQL_USER
              ? false
              : {skip: 'Incomplete PostgreSQL environment'};

var fmt = require('util').format;
var DbWatcher = require('../index');
var test = require('tap').test;

var pgCredentials = process.env.POSTGRESQL_USER;
if ((process.env.POSTGRESQL_PASSWORD || '').length > 0) {
  pgCredentials += ':' + process.env.POSTGRESQL_PASSWORD;
}
// template1 db is guaranteed to exist on all posgres servers
var POSTGRESQL_PARAM = fmt('postgres://%s@%s:%d/template1',
                           pgCredentials,
                           process.env.POSTGRESQL_HOST || 'localhost',
                           process.env.POSTGRESQL_PORT || 5432);

var TEST_TABLE_NAME = 'sdw_test';

var INSERT_PAYLOAD = {
  id: 18227,
  text: 'Text',
  bool: false,
  object: {a: 1, b:'B', c: true},
  ts: new Date(),
};

var INSERT_MSG_TARGET = {
  table: TEST_TABLE_NAME,
  op: 'INSERT',
  when: 'BEFORE',
  payload: INSERT_PAYLOAD,
  receivedAt: Date.now()
};

var DELETE_MSG_TARGET = {
  table: TEST_TABLE_NAME,
  op: 'DELETE',
  when: 'AFTER',
  payload: INSERT_PAYLOAD,
  receivedAt: Date.now()
};

var client;
var cleanupFn;
var sdw;

function closeConnection() {
  if (cleanupFn) cleanupFn();
  if (client) client.end();
  cleanupFn = null;
  client = null;
}

function createTable(t, client, callback) {
  var query = 'CREATE TEMPORARY TABLE IF NOT EXISTS ' + TEST_TABLE_NAME
      + ' (id integer primary key, text varchar, bool boolean, object varchar,'
      + ' ts timestamp);';
  client.query(query, function(err) {
    if (err) closeConnection();
    callback(err);
  });
}

function getDateTimeString(dt) {
  return dt.toISOString().replace('T', ' ').replace('Z', '')
      + '-' + dt.getTimezoneOffset() / 60;
}

function insertRecord(t, client, callback) {
  var query = 'INSERT INTO ' + TEST_TABLE_NAME + ' VALUES ('
      + INSERT_PAYLOAD.id + ','
      + '\'' + INSERT_PAYLOAD.text + '\'' + ','
      + '\'' + (INSERT_PAYLOAD.bool ? 't' : 'f') + '\'' + ','
      + '\'' + JSON.stringify(INSERT_PAYLOAD.object) + '\'' + ','
      + '\'' + getDateTimeString(INSERT_PAYLOAD.ts) + '\'' + ');';
  t.comment(query);
  client.query(query, function(err) {
    if (err) closeConnection();
    callback(err);
  });
}

function recordMatch(t, msg, target) {
  var table = t.equal(msg.table, target.table, target.op + ' equal on table');
  var op = t.equal(msg.op, target.op, target.op + ' equal on op');
  var when = t.equal(msg.when, target.when, target.op + ' equal on when');
  var id = t.equal(msg.payload.id, target.payload.id, target.op +
      ' equal on payload.id');
  var text = t.equal(msg.payload.text, target.payload.text, target.op +
      ' equal on payload.text');
  var bool = t.equal(msg.payload.bool, target.payload.bool, target.op +
      ' equal on payload.bool');
  var object = t.same(msg.payload.object, target.payload.object, target.op +
      ' same on payload.object');
  var foundTs = new Date(msg.payload.ts);
  var targetTs = new Date(target.payload.ts);
  var ts = t.equal(
      Date.parse(foundTs),
      Date.parse(targetTs),
      target.op + ' same on payload.ts');
  var receivedAt = t.ok(msg.receivedAt > target.receivedAt, target.op +
      ' ok on receivedAt');
  return table && op && when && id && text && bool
      && object && ts && receivedAt;
}

function insertRecordMatch(t, msg) {
  return recordMatch(t, msg, INSERT_MSG_TARGET);
}

function deleteRecord(t, client, callback) {
  var query = 'DELETE FROM ' + TEST_TABLE_NAME + ' WHERE id = ' +
      INSERT_PAYLOAD.id + ';';
  t.comment(query);
  client.query(query, function(err) {
    if (err) closeConnection();
    callback(err);
  });
}

function deleteRecordMatch(t, msg) {
  return recordMatch(t, msg, DELETE_MSG_TARGET);
}

test('Connect to DB', maybeSkip, function(t) {
  require('pg').connect(POSTGRESQL_PARAM, function(err, newClient, done) {
    t.ifError(err, 'Should connect without error');
    t.assert(newClient, 'Should yield a client object');
    client = newClient;
    cleanupFn = done;
    t.end();
  });
});

test('Create table', maybeSkip, function(t) {
  createTable(t, client, function(err) {
    t.ifError(err, 'Should create table without error');
    t.end();
  });
});

test('Create broadcaster', maybeSkip, function(t) {
  DbWatcher.Broadcast(client, [TEST_TABLE_NAME], function(err) {
    t.ifError(err, 'Should create broadcaster without error');
    if (err) closeConnection();
    t.end();
  });
});

test('Event emitter and watcher', maybeSkip, function(tt) {

  var insertMatched = false;
  var deleteMatched = false;
  var insertMatchedCL = false;
  var deleteMatchedCL = false;

  function changeListener(msg) {
    if (msg.op === 'INSERT') {
      insertMatchedCL = insertRecordMatch(tt, msg);
      tt.ok(insertMatchedCL, 'Inserted record should matchCL');
    }
    if (msg.op === 'DELETE') {
      deleteMatchedCL = deleteRecordMatch(tt, msg);
      tt.ok(deleteMatchedCL, 'Deleted record should matchCL');
    }
    if (insertMatched && deleteMatched &&
        insertMatchedCL && deleteMatchedCL) {
      sdw.close(function(err) {
        tt.ok(!err, 'DbWatcher should close');
        closeConnection();
        tt.end();
      });
    }
  }

  tt.test('Create watcher', maybeSkip, function(t) {
    t.doesNotThrow(function() {
      sdw = DbWatcher(client, changeListener);
    }, 'Should create watcher without error');
    t.end();
  });

  tt.test('Register watcher', maybeSkip, function(t) {
    sdw.watchTable(TEST_TABLE_NAME, function(err) {
      t.ifError(err, 'Should start watcher without error');
      if (err) closeConnection();
      t.end();
    });
  });

  tt.test('Start watcher', maybeSkip, function(t) {
    sdw.on(TEST_TABLE_NAME, function(msg) {
      tt.pass('Emitted:' + JSON.stringify(msg));
      if (msg.op === 'INSERT') {
        insertMatched = insertRecordMatch(tt, msg);
        tt.ok(insertMatched, 'Inserted record should match');
      }
      if (msg.op === 'DELETE') {
        deleteMatched = deleteRecordMatch(tt, msg);
        tt.ok(deleteMatched, 'Deleted record should match');
      }
      if (insertMatched && deleteMatched &&
          insertMatchedCL && deleteMatchedCL) {
        sdw.close(function(err) {
          tt.ok(!err, 'DbWatcher should close');
          closeConnection();
          tt.end();
        });
      }
    });
    t.end();
  });

  tt.test('Insert record', maybeSkip, function(t) {
    insertRecord(t, client, function(err) {
      t.ifError(err, 'Insert should work');
      t.end();
    });
  });

  tt.test('Delete record ', maybeSkip, function(t) {
    deleteRecord(t, client, function(err) {
      t.ifError(err, 'Delete should work');
      t.end();
    });
  });

});
