var config  = require('../../config/import.config');
var Logger  = require('../../storm/multilang/resources/src/lib/modules/logger');
var db      = require('./client');
var _       = require('underscore');
var async   = require('async');

var log = new Logger({
  scope : 'indexer',
  level : config.get('logLevel') || 0,
  file  : config.get('logFile')
});

/*
 * Indexer:  This module connects to couchDB and queries every view so
 * that couchDB will update the index to add the new ledgers.
 * 
 */

function Indexer () {
  var docs;
  var count = 0;
  
 /**
  *  pingCouchDB gets all of the design docs
  *  and queries one view per design doc to trigger
  *  couchdb's indexer process
  */
  this.pingCouchDB = function() {
    log.info("indexing couchDB views");
    
    if (docs && ++count < 5000) {
      updateViews(docs);
    
    //get the docs if we dont have them, or
    //every 5000 attempts (approx 7 hours)
    } else {
      count = 0;
      log.info("getting design docs");
      
      // list design docs
      db.nano.list({ startkey:'_design/', endkey:'_e' }, function(err, res){
        if (err) return log.error('problem getting design doc list: ' + err);
       
        var designDocIds = _.map(res.rows, function(row){ return row.key; });
    
        // get design docs
        db.nano.fetch({keys: designDocIds}, function(err, res){
          if (err) return log.error('problem getting design docs: ' + err);  
          
          docs = res.rows;
          updateViews(docs);
          
          //display which views are being indexed
          //this is currently broken and has different permissions
          if (0) {
            db.nano.request({path: '_active_tasks'}, function(err, res){
              if (err) {
                log.error(err);
                return;
              }
              
              res.forEach(function(process){
                if (process.design_document) { 
                  log.info('triggered update of ' + process.design_document);
                }
              });
            });
          }     
        });
      });
    }  
  }
  
  
  function updateViews (rows) {
    async.each(rows, function(row, asyncCallback) {

      if (!row.key || !row.doc) return asyncCallback();      

      var ddoc = row.key.slice(8),
        view   = Object.keys(row.doc.views)[0];

      // query one view per design doc
      db.nano.view(ddoc, view, { limit:1, reduce:false, stale:'update_after'}, function(err, res) {
        
        if (err) {
          log.error("invalid response triggering design doc: ", ddoc, err);
          return asyncCallback(err); 
        } 
        
        asyncCallback();         
      });

    }, 
    function(err, resp) {
      if (err) log.error(err); 
    });
  };
  
  return this; 
}

module.exports = Indexer();