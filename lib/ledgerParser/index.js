var Parser       = { };
var utils        = require('./utils');
var EPOCH_OFFSET = 946684800;
var SerializedObject = require('ripple-lib').SerializedObject;

Parser.affectedAccounts = require('./affectedAccounts');
Parser.offersExercised  = require('./offersExercised');
Parser.balanceChanges   = require('./balanceChanges');
Parser.accountsCreated  = require('./accountsCreated');

Parser.parse = function(ledger) {
  var data = { 
    ledgers         : { },
    transactions    : { },
    accountTx       : { },
    offersExercised : { },
    balanceChanges  : { },
    accountsCreated : { },
  };
  
  var transactions = ledger.transactions; 
  var closeTime    = ledger.close_time + EPOCH_OFFSET;
  var key          = ledger.ledger_hash + '|t|' + utils.formatTime(closeTime);
  var nOffers  = 0;
  var nChanges = 0;
  
  ledger.transactions = [];
  
  data.ledgers[key] = ledger;
  
  transactions.forEach(function(transaction) {
    ledger.transactions.push(transaction.hash);
    var meta = transaction.metaData;
    delete transaction.metaData;
    
    try {
      transaction.raw  = toHex(transaction);
      transaction.meta = toHex(meta);
      
    } catch (e) {
      log.error(e, transaction.ledger_index, transaction.hash);
      return;
    }
    
    transaction.metaData      = meta;
    transaction.executed_time = closeTime;
    transaction.tx_index      = transaction.metaData.TransactionIndex;
    transaction.tx_result     = transaction.metaData.TransactionResult;
    transaction.delivered     = transaction.metaData.DeliveredAmount;
    
    //memos
    addAccountsCreated(transaction);
    addOffersExercised(transaction);
    addBalanceChanges(transaction);
    addAccountTransactions(transaction);
    addTransaction(transaction);
  });
  
  console.log("ACCOUNTS CREATED");
  console.log(data.accountsCreated); 
  console.log("BALANCE CHANGES");
  console.log(data.balanceChanges);  
  console.log("OFFERS EXERCISED");
  console.log(data.offersExercised);
  console.log("ACCOUNT TX");
  console.log(data.accountTx);
  //console.log(data.transactions);
  console.log('done');
  return data;
  
  /**
   * addTransaction
   * format a transaction and add it to
   * the parsed data
   */
  
  function addTransaction (transaction) {
   var key = transaction.hash + 
    '|t|'  + utils.formatTime(transaction.executed_time) + 
    '|ti|' + transaction.tx_index;
    
    
    var tx = { 
      tx_type      : transaction.TransactionType,
      tx_result    : transaction.metaData.TransactionResult,
      ledger_index : ledger.ledger_index,
      ledger_hash  : ledger.ledger_hash,      
      tx_index     : transaction.tx_index,
      account      : transaction.Account,
      account_seq  : transaction.Sequence,
      raw          : transaction.raw,
      meta         : transaction.meta
    };
    
    data.transactions[key] = tx;
  }
  
  /**
   * addOffersExercised
   * add all offers exercised found
   */
  
  function addOffersExercised (transaction) {
    var list = Parser.offersExercised(transaction);
    
    list.forEach(function(ex, i) {
      var c1  = ex.base.currency + (ex.base.issuer ? "." + ex.base.issuer : "");
      var c2  = ex.counter.currency + (ex.counter.issuer ? "." + ex.counter.issuer : "");
      var key = c1 + '/' + c2 + 
        '|a|'  + ex.account +
        '|c|'  + ex.counterparty + 
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + transaction.tx_index +
        '|n|'  + (i+1) + //guarantee uniqueness
        '|h|'  + transaction.hash; 
      
      var value = {
        baseAmount    : ex.base.amount,
        counterAmount : ex.counter.amount,
        rate          : ex.rate,
      };
            
      data.offersExercised[key] = value;
      
      addAffectedAccount('exchange', ex.account, transaction);
      addAffectedAccount('exchange', ex.counterparty, transaction);
    });
  }
  
  /**
   * addAffectedAccount
   * add an account affected by the transaction
   * to the account transactions list
   */
  
  function addAffectedAccount (role, account, transaction) {
    var key = account    + 
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + transaction.tx_index +
        '|h|'  + transaction.hash;
    
    if (!data.accountTx[key]) {
      data.accountTx[key] = {
        roles : role ? [role] : []
      };
    
    } else if (role && data.accountTx[key].roles.indexOf(role) === -1) {
      data.accountTx[key].roles.push(role);
    }
  }
  
  /**
   * addAccountTransactions
   * add affected transactions to
   * account transactions lookup table
   */
  
  function addAccountTransactions (transaction) {
    var accounts = Parser.affectedAccounts(transaction);
    accounts.forEach(function(account) {
      if (account.roles.length) {
        account.roles.forEach(function(role) {
          addAffectedAccount(role, account.account, transaction); 
        });
        
      } else {
        addAffectedAccount(null, account.account, transaction);  
      }
    });
  }
  
  /**
   * addAccountBalanceChanges
   * add all balance changes found
   */
  
  function addBalanceChanges (transaction) {
    var list = Parser.balanceChanges(transaction);
    list.forEach(function(change, i) {
      var key = change.account + 
        '|c|'  + change.currency + 
        '|i|'  + (change.issuer || "")  +   
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + transaction.tx_index +
        '|n|'  + (i+1) + //guarantee uniqueness 
        '|h|'  + transaction.hash; 
      
      data.balanceChanges[key] = {
        change  : change.change,
        balance : change.balance,
        type    : change.type,
      }
      
      addAffectedAccount('balanceChange', change.account, transaction); 
      if (change.issuer) {
        addAffectedAccount('balanceChange', change.issuer, transaction); 
      }
    });
  }
  
  /**
   * addAccountsCreated
   * add any new accounts created
   * on this ledger
   */
  
  function addAccountsCreated (transaction) {
    var list = Parser.accountsCreated(transaction);
    list.forEach(function(account) {
      var key = account.account + 
        '|p|'  + account.parent +
        '|h|'  + transaction.hash +
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + transaction.tx_index;
      
      data.accountsCreated[key] = true;
      addAffectedAccount('created', account.account, transaction); 
      addAffectedAccount('parent', account.parent, transaction);
    });
  }
  
  //Convert json to binary/hex to store as raw data
  function toHex(input){
    return new SerializedObject.from_json(input).to_hex();
  }
  
};

module.exports = Parser;