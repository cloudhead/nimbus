var sys = require('sys'), path = require('path'), fs = require('fs');

require.paths.unshift(path.join(__dirname, '..'));

var nimbus = require('lib/nimbus');

sys.puts(path.basename(__filename));
sys.print('\n');

var db = new(nimbus.DB);
var dbpath = path.join(__dirname, 'db.nbus');

db.load(dbpath);

//
// 100'000 PUT operations
//
var count = 100000;

var docs = [{
    _id: 40,
    name: 'joseph',
    age: 32,
    traits: ['smart', 'awkward'],
}, {
    _id: 41,
    name: 'maria',
    age: 26,
    traits: ['tall', 'mysterious', 'funny'],
}, {
    _id: 42,
    name: 'tom',
    age: 16,
    traits: ['weird'],
}];

sys.puts('> inserting ' + count + ' ' + JSON.stringify(docs[0]).length + 'b documents.\n');

var start = Date.now();

for (var i = 0; i < count; i++) {
    db.put(docs[i % 3]);

    if (i % 10 === 0) {
        docs[i % 3]._id = i;
    }

    if (i === count / 2) {
        sys.puts('compacting...');
        db.compact(function (e) {
            sys.puts('done compacting.');
        });
    }
}

process.on('exit', function () {
    var time = (Date.now() - start) / 1000;
    sys.puts('runtime: ' + time + 's');
    sys.puts('rate: ' + parseInt(count / time) 
                      + '/s');
    sys.puts('db size: ' + fs.statSync(dbpath).size + 'b');
    db.destroy();
});
