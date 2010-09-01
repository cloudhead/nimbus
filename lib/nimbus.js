var fs = require('fs');
var sys = require('sys');
var path = require('path');

this.COMPACT_PATH = '/var/tmp/nimbus-compact';
this.RESERVED = ['_id', '_removed', '_ctime', '_atime'];
this.DEFAULT_OPTIONS = {
    compact: 4096 * 1024 * 1024
};

var nimbus = this;

//
// nimbus.DB
//
// The public interface to the data.
//
this.DB = function (options) {
    this.options = nimbus.clone(nimbus.DEFAULT_OPTIONS);
    this.store   = new(nimbus.Store)(this.options);

    for (var k in options) { this.options[k] = options[k] }
};
this.DB.prototype = new(function () {
    // API
    this.get     = store('get');
    this.put     = store('put');
    this.compact = store('compact');
    this.find    = store('find');
    this.filter  = store('filter');
    this.forEach = store('forEach');
    this.update  = store('update');
    this.exists  = store('exists');

    this.load = function (path) {
        return this.store.load(path);
    };

    this.destroy = function () {
        return this.store.destroy();
    };

    //
    // Wraps all Store methods. Takes care of raising an exception
    // if a callback wasn't supplied, and the error argument is non-null.
    // Also calls Store#load before calling the method, to make sure the
    // data is ready.
    //
    function store(method) {
        return function () {
            var that     = this,
                args     = Array.prototype.slice.call(arguments),
                callback = args[args.length - 1];

            if (typeof(callback) !== 'function') {
                callback = function (e) {
                    if (e) { throw new(Error)(e) }
                };
            } else {
                callback = args.pop();
            }

            args.push(function () {
                callback.apply(null, arguments);
            });

            if (! this.store.cache) {
                throw new(Error)('database not loaded.');
            }
            return this.store[method].apply(this.store, args);
        };
    }
});

//
// nimbus.Store
//
// The private interface to the data.
//
this.Store = function (options) {
    var that = this;

    this.path    = null;     // Path to the database file
    this.cache   = null;     // Holds the database in memory
    this.size    = null;     // Size, in bytes, of the database file
    this.fd      = null;     // The file descriptor to the database file
    this.options = options;

    this.compacting = false; // `true` if a compact operation is running
    this.dirty = [];         // List of accessed/created keys during compaction
};
this.Store.prototype = new(function () {
    //
    // Load the contents of the database in memory
    //
    this.load = function (pathname) {
        var that = this, stat, data;

        pathname = path.normalize(pathname);

        // Open the db file.
        if (this.fd === null) {
            try {
                stat = fs.statSync(pathname);
                this.size = stat.size;
            } catch (e) {
                this.size = 0;
            }
            this.fd   = fs.openSync(pathname, 'a+', 0666);
            this.path = pathname;
        }

        // Read the contents into `this.cache`.
        if (this.size > 0) {
            data = fs.readSync(this.fd, this.size, 0)[0];
            this.cache = data.split('\n').reduce(function (db, entry) {
                if (! entry) { return db }

                var obj = JSON.parse(entry);
                if (! obj._removed) {
                    db[obj._id] = new(that.Entry)(obj, true);
                }
                return db;
            }, {});
        } else {
            this.cache = {};
        }
        return this;
    };

    this.destroy = function () {
        if (this.fd) {
            return fs.unlinkSync(this.path);
        } else {
            return false;
        }
    };

    //
    // Cache entry. Wraps a document.
    //
    this.Entry = function (doc, persisted) {
        this.doc = doc;
        this.doc._ctime = this.doc._ctime || Date.now();
        this.persisted  = persisted || false;
    };
    this.Entry.prototype = {
        get id()    { return this.doc._id },
        get ctime() { return this.doc._ctime },
        get atime() { return this.doc._atime },
        update: function (doc) {
            this.doc = doc;
            this.doc._atime = Date.now();
        }
    };

    //
    // Compact the database, removing all duplicate entries.
    //
    this.compact = function (callback) {
        if (this.compacting) { return callback({ error: 'already compacting' }) }

        var that = this,
            tmp = nimbus.COMPACT_PATH + '-' + Date.now();

        // Open a temporary file, where we will put our compacted data.
        fs.open(tmp, 'a+', 0666, function (e, fd) {
            var keys = Object.keys(that.cache);

            that.compacting = true;
            that.persistKeys(fd, keys, function () {
                (function (remaining) {
                    var self = arguments.callee, oldfd;
                    if (remaining.length > 0) {
                        that.persistKeys(fd, remaining, function (e) {
                            self(that.dirty);
                        });
                    } else {
                        // Swap file descriptors
                        oldfd   = that.fd;
                        that.fd = fd;

                        fs.close(oldfd, function (e) {               if (e) { return callback(e) }
                            fs.rename(tmp, that.path, function (e) { if (e) { return callback(e) }
                                that.compacting = false;
                                that.dirty      = [];
                                callback(null);
                            });
                        });
                    }
                })(that.dirty);
            });
        });
    };
    //
    // Writes a bunch of keys to a file.
    //
    this.persistKeys = function (fd, keys, callback) {
        var length = keys.length,
            buffer = [],
            key,
            offset = 0, str;

        // Serialize the current in-memory data to a Buffer object
        while (key = keys.shift()) {
            buffer.push(JSON.stringify(this.cache[key].doc));
        }

        // Write the buffer to the temp file.
        fs.write(fd, buffer.join('\n') + '\n', 0, 'utf8', function (e) {
            if (e) { callback(e) }
            else   { callback(null, length) }
        });
    };

    //
    // Append a document to the database file.
    //
    this.write = function (obj, callback) {
        var that = this;
        fs.write(this.fd, JSON.stringify(obj) + '\n', null, 'utf8', function (e, written) {
            if (e) { return callback(e) }
            callback(null, written);
        });
    };

    //
    // Get a document from memory
    //
    this.get = function (id) {
        if (this.exists(id)) {
            return nimbus.clone(this.cache[id].doc);
        } else {
            return null;
        }
    };

    //
    // Check if a given key exists.
    //
    this.exists = function (id) {
        return this.cache.hasOwnProperty(id);
    };

    //
    // Write a document to memory and disk.
    // Can be used both for creating and updating.
    // If compaction is running, push the key to the
    // `this.dirty` list.
    //
    this.put = function (obj, callback) {
        var that = this, id = obj._id, entry;

        if (this.exists(id)) {
            this.cache[id].update(nimbus.clone(obj));
        } else {
            this.cache[id] = new(this.Entry)(obj);
        }

        this.write(obj, function (e, written) {
            if (e) { return callback(e) }
            callback(null, that.size += written);
            that.cache[id].persisted = true;

            if (that.size > that.options.compact) {
                that.compact();
            }

            // If a compaction is currently running, append the id being written
            // to a 'dirty' list, to make sure it'll be persisted.
            if (that.compacting && that.dirty.indexOf(id) === -1) {
                that.dirty.push(id);
            }
        });
    };

    //
    // Update an existing document with a partial document.
    //
    this.update = function (id, partial, callback) {
        var doc = this.get(id);

        if (doc) {
            for (var k in partial) { doc[k] = partial[k] }
            this.put(doc, callback);
        } else {
            throw new(Error)(id + " doesn't exist");
        }
    };

    //
    // Remove a document by id. Writes the document
    // with the '_removed' flag set to `true` and
    // deletes it from memory. The document will be lost
    // on Store#compact, and won't be reloaded on Store#load.
    //
    this.remove = function (id, callback) {
        var that = this;
        delete(this.cache[id]);
        that.update(id, { _removed: true }, callback);
    };

    //
    // Filter the documents through a function
    //
    this.filter = function (callback) {
        var keys = Object.keys(this.cache), doc, result = [];
        for (var i = 0; i < keys.length; i++) {
            doc = this.cache[keys[i]].doc;
            if (callback(doc)) {
                result.push(doc);
            }
        }
        return result;
    };

    //
    // Iterate through all documents, passing them to `callback`
    //
    this.forEach = function (callback) {
        var keys = Object.keys(this.cache);
        for (var i = 0; i < keys.length; i++) {
            callback(this.cache[keys[i]].doc);
        }
    };
    //
    // Find the first document which matches `condition`.
    //
    this.find = function (condition, callback) {
        var keys  = Object.keys(this.cache), obj,
            ckeys = Object.keys(condition);

        for (var i = 0; i < keys.length; i++) {
            obj = this.cache[keys[i]].doc;
            if (ckeys.every(function (k) { return obj[k] === condition[k] })) {
                return callback(obj);
            }
        }
        callback(null);
    };

    this.inspect = function () {
        var that = this;
        var ary = Object.keys(this.cache).map(function (k) {
            return JSON.stringify(that.cache[k].doc);
        });
        return ary.join('\n');
    };
});

//
// Perform a deep-copy
//
this.clone = function (obj) {
    var keys  = Object.keys(obj),
        clone = Array.isArray(obj) ? [] : {};

    for (var i = 0, key; i < keys.length; i++) {
        key = keys[i];

        if (obj[key] && typeof(obj[key]) === 'object') {
            clone[key] = arguments.callee(obj[key]);
        } else {
            clone[key] = obj[key];
        }
    }
    return clone;
};

