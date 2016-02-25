'use strict';

const request = require('request-promise');
const WebSocket = require('ws');
const validations = require('./validations');

const PEER_PORT_REGEX = /51235/g;
const WS_PORT = '51233';

const connections = {};

/**
 * requestSubscribe
 */

function requestSubscribe(ws) {
  ws.send(JSON.stringify({
    id: 1,
    command: 'subscribe',
    streams: [
      'validations'
    ]
  }));
}

/**
 * subscribe
 */

function subscribe(rippled) {

  const ip = 'ws://' + rippled.ipp.replace(PEER_PORT_REGEX, WS_PORT);

  // Skip addresses that are already connected
  if (connections[ip]) {
    if (connections[ip.ws]) {
      requestSubscribe(connections[this.url].ws);
    }
    return;
  }

  const ws = new WebSocket(ip);

  connections[ip] = {
    public_key: rippled.public_key,
    ws: ws
  };

  // handle error
  ws.on('close', function() {
    console.log(this.url, 'closed');
    if (this.url && connections[this.url]) {
      delete connections[this.url];
    }
  });

  // handle error
  ws.on('error', function(e) {

    // console.log(e);
    if (this.url && connections[this.url]) {
      if (connections[this.url].ws) {
        connections[this.url].ws.close();
      }
      delete connections[this.url];
    }
  });

  // subscribe and save new connections
  ws.on('open', function() {
    if (this.url &&
        connections[this.url] &&
        connections[this.url].ws) {

      requestSubscribe(connections[this.url].ws);
    }
  });

  // handle messages
  ws.on('message', function(message) {
    const data = JSON.parse(message);

    if (data.type === 'validationReceived') {
      data.reporter_public_key = connections[this.url].public_key;
      validations.handleValidation(data);

    } else if (data.error === 'unknownStream') {
      delete connections[this.url];
      console.log(data.error, this.url);
    }
  });
}

/**
 * getRippleds
 */

function getRippleds(api_url) {
  return request.get({
    url: `${api_url}/rippleds`,
    json: true
  });
}

/**
 * subscribeToRippleds
 */

function subscribeToRippleds(rippleds) {

  console.log('rippleds:', rippleds.length);
  console.log('connections:', Object.keys(connections).length);

  // Subscribe to validation websocket subscriptions from rippleds
  for (const rippled of rippleds) {
    if (!rippled.ipp) {
      continue;
    }

    subscribe(rippled);
  }

  return connections;
}

/**
 * start
 */

function start() {
  getRippleds('http://10.30.72.248:1234')
  .then(subscribeToRippleds)
  .catch(e => {
    console.log(e);
  });
}

// refresh connections
// every minute
setInterval(start, 60000);
start();
validations.start();


