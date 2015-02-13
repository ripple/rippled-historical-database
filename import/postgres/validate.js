var config   = require('../../config/import.config');
var log      = require('../../lib/log')('postgres_validator');
var HistoricalImport = require('./history');
var Postgres = require('./client');
var db       = new Postgres(config.get('postgres'));

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
      var ledgerIndex;

      if (err) {
        log.error(err);
        working = false;
        return;
        
      } else if (!ledger || !ledger.ledger_index) {
        log.info('no ledgers saved');
        working = false;
        return;
      } 
      
      ledgerIndex = parseInt(ledger.ledger_index, 10);
      
      if (ledgerIndex === stopIndex) {
        log.info('ledger not advanced!!!', stopIndex);
        working = false;
      
      } else if (!stopIndex) {
        log.info('setting stop index: ', ledgerIndex);
        stopIndex = ledgerIndex;
        working = false;
        
        //dont wait 90 seconds for the intial backfill
        setTimeout(validate, 15000);
        
      } else {      
        
        //history imports include the start and end ledgers
        //so account for that by adding and subtracting
        history.start(ledgerIndex - 1, stopIndex + 1, function(err, resp) {
          log.info('validated to:', ledgerIndex);
          stopIndex = ledgerIndex - 1;
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
