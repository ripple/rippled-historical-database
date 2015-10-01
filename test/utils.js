var request = require('request');
var assert = require('assert');
var utils = {};

  // This function helps build tests that iterate trough a list of results
  //
  utils.checkPagination = function (baseURL, initalMarker, testFunction, done) {
    var url= baseURL + (initalMarker ? '&marker='+initalMarker : '');
    request({
        url: url,
        json: true,
      },
        function (err, res, body) {
          assert.ifError(err);
          assert.strictEqual(res.statusCode, 200);
          assert.strictEqual(typeof body, 'object');
          assert.strictEqual(body.result, 'success');
          assert(body.count > 1, 'must be at least 2 : ' + body.count + ' found');
          return checkIter(body, 0, body.count, initalMarker);
    });

    function checkIter(ref, i, imax, marker) {
      if(i < imax) {
        var url= baseURL + (marker ? '&limit=1&marker='+marker : '&limit=1');
        request({
            url: url,
            json: true,
          },
            function (err, res, body) {
              assert.ifError(err);
              assert.strictEqual(res.statusCode, 200);
              assert.strictEqual(typeof body, 'object');
              assert.strictEqual(body.result, 'success');
              testFunction(ref, i, body);
              checkIter(ref, i+1, imax, body.marker);
        });
      } else {
        done();
      }
    }
  };


module.exports = utils;
