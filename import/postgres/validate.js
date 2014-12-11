var config   = require('../../config/import.config');
var log      = require('../../lib/log')('postgres_validator');
var HistoricalImport = require('./history');
var db = require('./client');

//if no earliest saved or earliest saved is greater than
//the genesis ledger, start backfilling from earliest saved/validated
var Validator = function() {
  var working  = false;
  var history  = new HistoricalImport();
  var self     = this;
  var interval;
  var stopIndex;
  
  function validate () {
    if (working) {
      return false;
    }
    
    working = true;
    log.info('Starting validation process...');
    db.getLatestLedger(function(err, ledger) {
      
      if (err) {
        log.error(err);
        working = false;
        
      } else if (!ledger || !ledger.ledger_index) {
        log.info('no ledgers saved');
        working = false;
      
      } else if (ledger.ledger_index === stopIndex) {
        log.info('ledger not advanced!!!', stopIndex);
        working = false;
      
      } else if (!stopIndex) {
        log.info('setting stop index: ', ledger.ledger_index);
        stopIndex = ledger.ledger_index;
        working = false;
        
      } else {      
        history.start(ledger.ledger_index, stopIndex, function(err, resp) {
          log.info('validated to:', ledger.ledger_index);
          stopIndex = ledger.ledger_index;
          working = false;
        });
      }
    });
  }
  
  this.start = function () {
    if (!interval) {
      interval = setInterval(validate, 90 * 1000);
      validate();
    }
  };
  
  this.stop = function () {
    if (interval) {
      clearInterval(interval);
      interval = null;
    }
  };
  
  return this;
}

module.exports = Validator;
