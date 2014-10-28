var log  = require('../../lib/log')('Ledger_Parser');
var SerializedObject = require('ripple-lib').SerializedObject;
var utils = require('./utils');

var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var Parser       = { };

Parser.affectedAccounts = require('./affectedAccounts');
Parser.offersExercised  = require('./offersExercised');
Parser.balanceChanges   = require('./balanceChanges');
Parser.accountsCreated  = require('./accountsCreated');

Parser.parse = function(ledger) {
  var data = { 
    ledgers             : { },
    transactions        : { },
    transactionsByIndex : { },
    accountTx           : { },
    accountTxByIndex    : { },
    offersExercised     : { },
    balanceChanges      : { },
    payments            : { },
    accounts            : { },
    accountsCreated     : { },
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
    
    transaction.metaData        = meta;
    transaction.ledger_hash     = ledger.ledger_hash;
    transaction.ledger_index    = ledger.ledger_index;
    transaction.executed_time   = closeTime;
    transaction.tx_index        = transaction.metaData.TransactionIndex;
    transaction.tx_result       = transaction.metaData.TransactionResult;
    if (transaction.TransactionType === 'Payment') {
      transaction.DeliveredAmount = transaction.metaData.DeliveredAmount || transaction.Amount;
    }
    
    //memos
    addAccountsCreated(transaction);
    addOffersExercised(transaction);
    addBalanceChanges(transaction);
    addAccountTransactions(transaction);
    addTransaction(transaction);
  });
/*  
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
*/

  return data;
  
  /**
   * addTransaction
   * format a transaction and add it to
   * the parsed data
   */
  
  function addTransaction (transaction) {
   var key = transaction.hash + 
    '|t|'  + utils.formatTime(transaction.executed_time) + 
    '|ti|' + pad(transaction.tx_index, I_PAD);
    
    data.transactions[key] = transaction;
    
    //add to lookup table
    key = pad(transaction.ledger_index, LI_PAD) + '|ti|' + pad(transaction.tx_index, I_PAD);
    data.transactionsByIndex[key] = {tx_hash: transaction.hash};
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
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD) +
        '|n|'  + pad(ex.nodeIndex, I_PAD); //guarantee uniqueness

      
      var value = {
        baseAmount    : ex.base.amount,
        counterAmount : ex.counter.amount,
        rate          : ex.rate,
        account       : ex.account,
        counterparty  : ex.counterparty,
        tx_hash       : transaction.hash
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
    var key = account +
        '|r|'  + transaction.tx_result + 
        '|tp|' + transaction.TransactionType +
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD);
    
    if (!data.accountTx[key]) {
      data.accountTx[key] = {
        roles     : role ? [role] : [],
        tx_hash   : transaction.hash,
      };
    
      //add to lookup table
      key = account + '|li|' + pad(transaction.ledger_index, LI_PAD) + '|ti|' + pad(transaction.tx_index, I_PAD);
      data.accountTxByIndex[key] = {tx_hash: transaction.hash};
      
    } else if (role && data.accountTx[key].roles.indexOf(role) === -1) {
      data.accountTx[key].roles.push(role);
    }
    
    if (!data.accounts[account]) {
      data.accounts[account] = { };
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
        '|ti|' + pad(transaction.tx_index, I_PAD) +
        '|ni|' + pad(change.nodeIndex, I_PAD); //guarantee uniqueness  
      
      data.balanceChanges[key] = {
        change  : change.change,
        amount  : change.amount,
        fee     : change.fee,
        balance : change.balance,
        type    : change.type,
      }
      
      addAffectedAccount('balanceChange', change.account, transaction); 
      if (change.issuer) {
        addAffectedAccount('balanceChange', change.issuer, transaction); 
      }
    });
    
    if (transaction.TransactionType === 'Payment') {
      var payment = { };
      list.forEach(function(change) {

        if (change.type === 'network fee') {
          payment.networkFee = 0 - change.fee;
        }
        
        if (change.type !== 'payment') {
          return;
        }
        
        if (change.account === transaction.Account) {
          payment.sender = change.account;
          payment.amountSent = {
            amount   : 0 - change.amount,
            currency : change.currency
          };
          
          if (change.issuer) {
            payment.amountSent.issuer = change.issuer;
          }
          
          payment.senderBalance = change.balance;
        
        } else if (change.account === transaction.Destination) {
          payment.receiver = change.account;
          payment.amountReceived = {
            amount   : change.amount,
            currency : change.currency
          };
          
          if (change.issuer) {
            payment.amountReceived.issuer = change.issuer;
          }
          
          payment.receiverBalance = change.balance;          
        }
      });
      
      //issuer is the sender
      if (payment.sender && !payment.receiver) {
        payment.amountReceived = payment.amountSent;
        payment.receiver = payment.amountReceived.issuer;
        payment.receiverBalance = 0 - payment.senderBalance;
      
      //issuer is the receiver
      } else if (payment.receiver && !payment.sender) {
        payment.amountSent = payment.amountReceived;
        payment.sender = payment.amountSent.issuer;
        payment.senderBalance = 0 - payment.receiverBalances;      
      }
      
      payment.tx_hash = transaction.hash;
    }
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
        '|t|'  + utils.formatTime(transaction.executed_time) + 
        '|ti|' + pad(transaction.tx_index, I_PAD);
      
      data.accountsCreated[key] = {tx_hash : transaction.hash};
      
      key = account.account;
      data.accounts[key] = {
        parent    : account.parent,
        created   : transaction.executed_time,
        create_tx : transaction.hash,
      };
      
      addAffectedAccount('created', account.account, transaction); 
      addAffectedAccount('parent', account.parent, transaction);
    });
  }
  
  //Convert json to binary/hex to store as raw data
  function toHex(input){
    return new SerializedObject.from_json(input).to_hex();
  }
  
  function pad(num, size) {
    var s = num+"";
    if (!size) size = 10;
    while (s.length < size) s = "0" + s;
    return s;
  }
  
};

module.exports = Parser;