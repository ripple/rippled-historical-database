var smoment = require('../lib/smoment');
var moment = require('moment');
var assert  = require('assert');


describe('smoment tests', function() {

  before(function(done) {
    done();
  });

  it('should check date/time are parsed correctly', function(done) {

    assert.strictEqual(smoment('abcd'), undefined);
    assert.strictEqual(smoment(0).format(), '1970-01-01T00:00:00Z');
    assert.strictEqual(smoment().format(), moment().utc().format('YYYY-MM-DDTHH:mm:ss[Z]')); // This might fail
    assert.strictEqual(smoment(946684800).format(), '2000-01-01T00:00:00Z');               // Ripple Epoch
    assert.strictEqual(smoment('1234567890').format(), '2009-02-13T23:31:30Z');            // 10 digit timestamps only
    assert.strictEqual(smoment('946684d800'), undefined);
    assert.strictEqual(smoment('2015-03-04 18:22:33'), undefined);
    assert.strictEqual(smoment('2015-03-04T18:22:33').format(), '2015-03-04T18:22:33Z');
    done();
  });

  it('should check that start/end rows are computed correctly', function(done) {
    assert.strictEqual(smoment('2015').hbaseFormatStopRow(), smoment('2016').hbaseFormatStartRow());
    assert.strictEqual(smoment('2015-04').hbaseFormatStopRow(), smoment('2015-05').hbaseFormatStartRow());
    assert.strictEqual(smoment('2015-04').hbaseFormatStopRow(), smoment('2015-05-01T00:00:00').hbaseFormatStartRow());
    assert.strictEqual(smoment('2015-04-10').hbaseFormatStopRow(), smoment('2015-04-11T00:00:00').hbaseFormatStartRow());
    assert.strictEqual(smoment('2015-04-10T13').hbaseFormatStopRow(), smoment('2015-04-10T14:00:00').hbaseFormatStartRow());
    assert.strictEqual(smoment('2015-04-10T13:12').hbaseFormatStopRow(), smoment('2015-04-10T13:13:00').hbaseFormatStartRow());
    assert.strictEqual(smoment('2015-04-10T13:12:42').hbaseFormatStopRow(), smoment('2015-04-10T13:12:43').hbaseFormatStartRow());
    done();
  });

  it('should check the ctor', function(done) {
    var m = moment('2015-01-02T13:45:32Z');
    assert.strictEqual(m.isSame(smoment(m).moment), true);
    var sm= smoment(1234);
    assert.strictEqual(sm.moment.isSame(smoment(sm).moment), true);
    done();
  });

})


