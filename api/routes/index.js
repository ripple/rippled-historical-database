'use strict';

var Routes = { };

Routes.getLedger = require('./getLedger')
Routes.getTransactions = require('./getTransactions')
Routes.accountTxSeq = require('./accountTxSeq')
Routes.accountTransactions = require('./accountTransactions')
Routes.accountExchanges = require('./accountExchanges')
Routes.accountPayments = require('./accountPayments')
Routes.accountStats = require('./accountStats')
Routes.accountBalances = require('./accountBalances')
Routes.accountOrders = require('./accountOrders')
Routes.accountReports = require('./accountReports')
Routes.getChanges = require('./accountBalanceChanges')
Routes.getPayments = require('./getPayments')
Routes.getExchanges = require('./getExchanges')
Routes.getExchangeRate = require('./getExchangeRate')
Routes.normalize = require('./normalize')
Routes.reports = require('./reports')
Routes.stats = require('./stats')
Routes.accounts = require('./accounts')
Routes.getAccount = require('./getAccount')
Routes.getLastValidated = require('./getLastValidated')
Routes.checkHealth = require('./checkHealth')
Routes.capitalization = require('./capitalization')
Routes.activeAccounts = require('./activeAccounts')
Routes.maintenance = require('./maintenance')
Routes.network = require('./network')
Routes.gateways = require('./gateways')

module.exports = Routes
