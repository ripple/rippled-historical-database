module.exports = {
  "logLevel" : 3,
  "logFile"  : "nodejs.log",
  "ripple" : {
    "trace"                 : false,
    "allow_partial_history" : false,
    "servers" : [
      { "host" : "s-west.ripple.com", "port" : 443, "secure" : true },
      { "host" : "s-east.ripple.com", "port" : 443, "secure" : true }
    ]
  },
  "hbase" : {
    "prefix" : "beta2_",
    "host"   : "54.172.205.78",
    "port"   : 9090
  }
}