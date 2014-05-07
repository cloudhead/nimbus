var path = require('path'),
    util = require('util'),
    sys = require('sys'),
    fs = require('fs');

var vows = require('vows'),
    assert = require('assert');

var DBPATH = path.join(__dirname, 'database.nbus');

require.paths.unshift(path.join(__dirname, '..'));

var nimbus = require('lib/nimbus');

// Setup database
fs.writeFileSync(DBPATH, [
    {_id: 0, name: 'bob'},  // filter
    {_id: 1, name: 'jon'},  // remove
    {_id: 2, name: 'bill'}, // filter
    {_id: 3, name: 'tuna'}, // get
    {_id: 4, name: 'pope'}, // update
 //       5                 // put
 //       6                 // changes (put)
    {_id: 7, name: 'tux'},  // changes (update)
    {_id: 8, name: 'mer'}   // changes (remove)
].map(JSON.stringify).join('\n') + '\n');

vows.describe('nimbus').addBatch({
    'A nimbus.DB instance': {
        topic: new(nimbus.DB),

        'should be inspectable': function (db) {
            assert.doesNotThrow (function () { util.inspect(db) }, TypeError);
        },

        'with an existing database loaded': {
            topic: function (db) {
                db.load(DBPATH);
                return this.db = db;
            },
            'when performing a *get* with an id': {
                topic: function (db) {
                    return db.get(3);
                },
                'should return the document with that id': function (doc) {
                    assert.isNotNull (doc);
                    assert.equal     (doc.name, 'tuna');
                }
            },
            'when performing a *put* with a new id': {
                topic: function (db) {
                    db.put({_id: 5, name: 'mana'}, this.callback);
                },
                'should store the document in the cache': function (res) {
                    assert.isObject (this.db.store.cache[5]);
                    assert.equal    (this.db.store.cache[5].doc.name, 'mana');
                },
                'should store the document on disk': function (res) {
                    db = new(nimbus.DB);
                    db.load(DBPATH);

                    assert.isObject (db.get(5));
                    assert.equal    (db.get(5).name, 'mana');
                },
                'should set the *persisted* flag to `true`': function (res) {
                    assert.isTrue   (this.db.store.cache[5].persisted);
                }
            },
            'when asking if a key exists': {
                topic: function (db) {
                    return db.exists(2);
                },
                'should reply `true` or `false`': function (res) {
                    assert.isTrue (res);
                }
            },
            'when performing an *update*': {
                topic: function (db) {
                    db.update(4, {name: 'Benedictus'}, this.callback);
                },
                'should modify the document in cache': function (res) {
                    assert.isObject (this.db.store.cache[4]);
                    assert.equal    (this.db.store.cache[4].doc.name, 'Benedictus');
                },
                'should modify the document on disk': function (res) {
                    db = new(nimbus.DB);
                    db.load(DBPATH);

                    assert.isObject (db.get(4));
                    assert.equal    (db.get(4).name, 'Benedictus');
                }
            },
            'when performing a *remove*': {
                topic: function (db) {
                    db.remove(1, this.callback);
                },
                'should remove the document from cache': function (res) {
                    assert.isUndefined (this.db.store.cache[1]);
                },
                'should not load the document on future db load': function (res) {
                    db = new(nimbus.DB);
                    db.load(DBPATH);

                    assert.isNull (db.get(1));
                }
            },
            'when performing a *filter*': {
                topic: function (db) {
                    return db.filter(function (doc) {
                        return doc.name.match(/^b/);
                    });
                },
                'should return documents for which the callback returns true': function (res) {
                    names = res.map(function (doc) { return doc.name });
                    assert.include(names, 'bob' );
                    assert.include(names, 'bill');
                }
            },
        },
        'when registering for *changes* and creating a document': {
            topic: function () {
                var that = this;
                db = new(nimbus.DB);
                db.load(DBPATH);
                db.changes(function (eventType, doc) { that.callback(null, eventType, doc) });
                db.put({_id: 6, name: 'voldemort'});
            },
            'should be notified of the new document': function (err, eventType, doc) {
                assert.equal    (eventType, 'put');
                assert.isObject (doc);
                assert.equal    (doc.name, 'voldemort');
                assert.equal    (doc._id, 6);
            }
        },
        'when registering for *changes* and updating a document': {
            topic: function () {
                var that = this;
                db = new(nimbus.DB);
                db.load(DBPATH);
                db.changes(function (eventType, doc) { that.callback(null, eventType, doc) });
                db.update(7, {name: 'Beastie'});
            },
            'should be notified of the updated document': function (err, eventType, doc) {
                assert.equal    (eventType, 'update');
                assert.isObject (doc);
                assert.equal    (doc.name, 'Beastie');
                assert.equal    (doc._id, 7);
            }
        },
        'when registering for *changes* and removing a document': {
            topic: function () {
                var that = this;
                db = this.db = new(nimbus.DB);
                db.load(DBPATH);
                db.changes(function (eventType, doc) { that.callback(null, eventType, doc) });
                assert.isObject (db.get(8));
                db.remove(8);
            },
            'should be notified of the updated document': function (err, eventType, doc) {
                assert.equal    (eventType, 'remove');
                assert.isObject (doc);
                assert.equal    (doc.name, 'mer');
                assert.equal    (doc._id, 8);
            }
        }
    }
}).export(module);
