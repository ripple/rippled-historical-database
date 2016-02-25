var exec = require('child_process').exec;

function updateMetric(command) {
  exec(command, function callback(e, stdout, stderr) {
    if (e) {
      console.log(e);
    }

    if (stderr) {
      console.log(stderr);
    }

    if (stdout) {
      console.log(stdout);
    }
  });
}

function updateMetrics() {
  updateMetric('node scripts/tradeVolume --top --save');
  updateMetric('node scripts/paymentVolume --top --save');
  updateMetric('node scripts/issuedValue --top --save');
}


setInterval(updateMetrics, 3 * 60 * 1000);
updateMetrics();
