var config = require('./config');
var nodemailer = require('nodemailer');
var transporter = nodemailer.createTransport();
var to = config.get('recipients');
var exec = require('child_process').exec;
var log;

/**
 * notify
 */

function notify(message, callback) {
  var params = {
    from: 'Storm Import<storm-import@ripple.com>',
    to: to,
    subject: 'uncaughtException',
    html: 'The import topology received ' +
      'an uncaugt exception error: <br /><br />\n' +
      '<blockquote><pre>' + message + '</pre></blockquote><br />\n'
  };

  transporter.sendMail(params, callback);
}

/**
 * killTopology
 */

function killTopology() {
  exec('storm kill "ripple-ledger-importer"',
       function callback(e, stdout, stderr) {
    if (e) {
      log.error(e);
    }

    if (stderr) {
      log.error(stderr);
    }

    if (stdout) {
      log.info(stdout);
    }
  });
};

module.exports = function(logger) {
  log = logger;

  // handle uncaught exception
  process.on('uncaughtException', function(e) {
    log.error(e);
    log.error(e.stack);

    //send notification
    notify(e.stack, function(err, info) {
      if (err) {
        log.error(err);
      } else {
        log.info('Notification sent: ', info.accepted);
      }
    });

    //kill the topology
    killTopology();
  });
}
