nimbus
======

> nimble, durable, document store.

synopsis
--------

    var nimbus = require('nimbus');
    var db = new(nimbus.DB);

    db.load('./database.nimbus');
    db.put({ _id: 32, name: 'joseline', profession: 'botanist' });
    db.get(32).name; // 'joseline'
    db.update(32, { profession: 'florist' });
    db.get(32).profession; // 'florist'

    db.remove(32);
    db.get(32); // null

    db.filter(function (row) {
        return row.name.match(/^[Jj]/);
    }); // [{ _id: 32, name: 'joseline', profession: 'florist' }]

    db.put({ _id: 45, name: 'locke' }, function (err) {
        if (! err) {
            // The data has been persisted to disk.
        }
    }); // The data is in memory.

More information coming soon.

license
-------

See `LICENSE`.

Copyright (c) 2010 - Alexis Sellier
