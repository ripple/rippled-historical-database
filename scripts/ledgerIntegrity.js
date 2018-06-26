const config = require('../config');
config.set('logLevel', 2);
const hbase = require('../lib/hbase');
const moment = require('moment');
const batchSize = config.get('batchSize') || 100;
const stop = config.get('stop') || Infinity;
let index = config.get('start') || 32570;

const getLedger = (index) => {
  return new Promise((resolve, reject) => {
    hbase.getLedger({
      ledger_index: index,
      expand: true
    }, function(error, ledger) {
      if (error) {
        resolve({
          error: error,
          index: index
        });

      } else if (!ledger) {
         resolve({
          error: 'not found',
          index: index
        });

      } else if (!ledger.transactions && !Number(ledger.transaction_hash)) {
        resolve();

      } else {
        ledger.transactions.forEach(tx => {
          if (isNaN(tx.ledger_index)) {
            resolve({
              error: 'invalid ledger index',
              index: index,
              tx: tx
            });
          } else if (!moment(tx.date).isValid()) {
            resolve({
              error: 'invalid ledger index',
              index: index,
              tx: tx
            });
          }
        })

        resolve();
      }
    });
  });
}

const getBatch = () => {
  let i = batchSize;
  const tasks = [];
  let last = index + i - 1;
  if (last > stop) {
    last = stop
  }

  console.log(`${moment.utc().format()} checking ledgers ${index}-${last}`);
  while(i--) {
    if (index < stop) {
      tasks.push(getLedger(index++));
    }
  }

  Promise.all(tasks)
  .then(resp => {
    resp.forEach(d => {
      if (d) {
        throw(d);
      }
    });

    if (index === stop) {
      console.log('done');
      process.exit();
    }

    setImmediate(getBatch);
  })
  .catch(error => {
    console.log(error);
    process.exit(1)
  });
}


getBatch()


