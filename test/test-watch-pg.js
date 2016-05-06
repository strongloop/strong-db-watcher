// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: strong-db-watcher
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

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
var TEST_TABLE_NAME_2 = 'sdw_test_2';
var TEST_TABLE_NAME_3 = 'sdw_test_3';

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
  var query2 = fmt('CREATE TEMPORARY TABLE %s AS TABLE %s;',
                 TEST_TABLE_NAME_2, TEST_TABLE_NAME);
  var query3 = fmt('CREATE TEMPORARY TABLE %s AS TABLE %s;',
                 TEST_TABLE_NAME_3, TEST_TABLE_NAME);
  client.query(query + query2 + query3, function(err) {
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
  t.comment('No need to start a broadcaster separately.');
  t.end();
});

test('Event emitter and watcher', maybeSkip, function(tt) {

  var insertMatched = null;
  var deleteMatched = null;
  var insertMatchedCL = null;
  var deleteMatchedCL = null;

  function changeListener(msg) {
    tt.comment('emitted CL: %j', msg);
    if (msg.op === 'INSERT') {
      tt.equal(insertMatchedCL, null, 'insertRecordMatch CL called once');
      insertMatchedCL = insertRecordMatch(tt, msg);
    }
    if (msg.op === 'DELETE') {
      tt.equal(deleteMatchedCL, null, 'deleteRecordMatch CL called once');
      deleteMatchedCL = deleteRecordMatch(tt, msg);
    }
    if (insertMatched && deleteMatched &&
        insertMatchedCL && deleteMatchedCL) {
      tt.pass('Data insert/delete succeeds CL');
      sdw.unwatchTable(TEST_TABLE_NAME, function(err) {
        tt.equal(Object.keys(sdw._tableSchema).length, 1,
            'Table schema released CL');
        tt.ifError(err, 'unwatchTable succeeds CL');
        sdw.close(function(err) {
          tt.equal(sdw._tableSchema, null, 'Table schema null CL');
          tt.ifError(err, 'DbWatcher closes CL');
          closeConnection();
          tt.end();
        });
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
      t.equal(Object.keys(sdw._tableSchema).length, 1,
          'Table schema created');
      t.ifError(err, 'watchTable succeeds');
      if (err) closeConnection();
      t.end();
    });
  });

  tt.test('Register 2nd watcher', maybeSkip, function(t) {
    sdw.watchTable(TEST_TABLE_NAME_2, function(err) {
      t.equal(Object.keys(sdw._tableSchema).length, 2,
          'Table schema created');
      t.ifError(err, 'watchTable 2nd succeeds');
      if (err) closeConnection();
      t.end();
    });
  });

  tt.test('Start watcher', maybeSkip, function(t) {

    function watchFn(msg) {
      tt.pass('Emitted:' + JSON.stringify(msg));
      if (msg.op === 'INSERT') {
        tt.equal(insertMatched, null, 'insertRecordMatch called once');
        insertMatched = insertRecordMatch(tt, msg);
      }
      if (msg.op === 'DELETE') {
        tt.equal(deleteMatched, null, 'deleteRecordMatch called once');
        deleteMatched = deleteRecordMatch(tt, msg);
      }
      if (insertMatched && deleteMatched &&
          insertMatchedCL && deleteMatchedCL) {
        tt.pass('Data insert/delete succeeds');
        sdw.unwatchTable(TEST_TABLE_NAME, function(err) {
          tt.ifError(err, 'unwatchTable succeeds');
          tt.equal(Object.keys(sdw._tableSchema).length, 1,
              'Table schema released');
          tt.ok(!sdw.isWatching(TEST_TABLE_NAME, watchFn),
              'not watching ' + TEST_TABLE_NAME);
          sdw.close(function(err) {
            tt.ifError(err, 'DbWatcher closes');
            tt.equal(sdw._tableSchema, null, 'Table schema null');
            closeConnection();
            tt.end();
          });
        });
      }
    }

    sdw.on(TEST_TABLE_NAME, watchFn);
    t.ok(sdw.isWatching(TEST_TABLE_NAME, watchFn),
        'watching ' + TEST_TABLE_NAME);
    t.ok(!sdw.isWatching(TEST_TABLE_NAME_2, watchFn),
        'not watching ' + TEST_TABLE_NAME_2);
    sdw.on(TEST_TABLE_NAME_3, watchFn);
    t.ok(!sdw.isWatching(TEST_TABLE_NAME_3, watchFn),
        'not watching ' + TEST_TABLE_NAME_3);
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

