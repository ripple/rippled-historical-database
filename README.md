![Travis Build Status](https://travis-ci.org/ripple/rippled-historical-database.svg?branch=develop)

rippled Historical Database
==========================

The `rippled` Historical Database provides access to raw Ripple transactions that are stored in a database. This provides an alternative to querying `rippled` itself for transactions that occurred in older ledgers, so that the `rippled` server can maintain fewer historical ledger versions and use fewer server resources.

Ripple Labs provides a live instance of the `rippled` Historical Database API with as complete a transaction record as possible at the following address:

`history.ripple.com`

You can also [install and run your own instance of the Historical Database](#running-the-historical-database).


## API Methods ##

The `rippled` Historical Database provides a REST API, with the following methods:

V1 Methods:
* [Get Account Transaction History - `GET /v1/accounts/{:address}/transactions`](#get-account-transaction-history)
* [Get Transaction By Account and Sequence - `GET /v1/accounts/{:address}/transactions/{:sequence}`](#get-transaction-by-account-and-sequence)
* [Get Ledger - `GET /v1/ledgers/{:ledger_identifier}`](#get-ledger)
* [Get Transaction - `GET /v1/transactions/{:hash}`](#get-transaction)

V2 Methods:
* [Get Ledger - `GET /v2/ledgers/{:ledger_identifier}`](#get-ledger-v2)
* [Get Transaction - `GET /v2/transactions/{:hash}`](#get-transaction-v2)
* [Get Transactions - `GET /v2/transactions/`](#get-transactions)
* [Get Payments - `GET /v2/payments/:currency`](#get-payments)
* [Get Exchanges - `GET /v2/exchanges/:base/:counter`](#get-exchanges)
* [Get Exchange Rates - `GET /v2/exchange_rates/:base/:counter`](#get-exchange-rates)
* [Get Normalization - `GET /v2/normalize`](#normalize)
* [Get Reports - `GET /v2/reports/`](#get-reports)
* [Get Stats - `GET /v2/stats/`](#get-stats)
* [Get Capitalization - `GET /v2/capitalization/:currency/:issuer`](#get-capitalization)
* [Get Active Accounts - `GET /v2/active_accounts/:base/:counter`](#get-active-accounts)
* [Get Exchange Volume - `GET /v2/network/exchange_volume`](#get-exchange-volume)
* [Get Payment Volume - `GET /v2/network/payment_volume`](#get-payment-volume)
* [Get Issued Value - `GET /v2/network/issued_value`](#get-issued-value)
* [Get Accounts - `GET /v2/accounts`](#get-accounts)

V2 Account Methods:
* [Get Account - `GET /v2/accounts/{:address}`](#get-account)
* [Get Account Balances - `GET /v2/accounts/{:address}/balances`](#get-account-balances)
* [Get Account Transaction History - `GET /v2/accounts/{:address}/transactions`](#get-account-transaction-history-v2)
* [Get Transaction By Account and Sequence - `GET /v2/accounts/{:address}/transactions/{:sequence}`](#get-transaction-by-account-and-sequence-v2)
* [Get Account Payments - `GET /v2/accounts/{:address}/payments`](#get-account-payments)
* [Get Account Exchanges - `GET /v2/accounts/{:address}/exchanges`](#get-account-exchanges)
* [Get Account Balance Changes - `GET /v2/accounts/{:address}/balance_changes`](#get-account-balance-changes)
* [Get Account Reports - `GET /v2/accounts/{:address}/reports`](#get-account-reports)

# API Objects #

## Transaction Objects ##

Transactions have two formats - a compact "binary" format where the defining fields of the transaction are encoded as strings of hex, and an expanded format where the defining fields of the transaction are nested as complete JSON objects.

### Full JSON Format ###

| Field | Value | Description |
|-------|-------|-------------|
| hash  | String - Transaction Hash | An identifying SHA-512 hash unique to this transaction, as a hex string. |
| date  | String - IS0 8601 UTC Timestamp | The time when this transaction was included in a validated ledger. |
| ledger_index | Number - Ledger Index | The sequence number of the ledger that included this ledger. |
| tx    | Object | The fields of this transaction object, as defined by the [Transaction Format](https://ripple.com/build/transactions) |
| meta  | Object | Metadata about the results of this transaction. |

### Binary Format ###

| Field | Value | Description |
|-------|-------|-------------|
| hash  | String - Transaction Hash | An identifying SHA-512 hash unique to this transaction, as a hex string. |
| date  | String - IS0 8601 UTC Timestamp | The time when this transaction was included in a validated ledger. |
| ledger_index | Number - Ledger Index | The sequence number of the ledger that included this ledger. |
| tx    | String | The binary data that represents this transaction, as a hex string. |
| meta  | Object | The binary data that represents this transaction's metadata, as a hex string. |

## Ledger Objects ##

A "ledger" is one block of the shared global ledger. Each ledger object has the following fields:

| Field        | Value | Description |
|--------------|-------|-------------|
| ledger_hash  | String - Transaction Hash | An identifying hash unique to this ledger, as a hex string. |
| ledger_index | Number (Unsigned Integer) - Ledger Index | The sequence number of the ledger. Each new ledger has a ledger index 1 higher than the ledger that came before it. |
| parent_hash  | String - Transaction Hash | The identifying hash of the previous ledger. |
| total_coins  | Unsigned Integer | The total number of drops of XRP still in existence at the time of the ledger. (Each "drop" is 100,000 XRP.) |
| close\_time\_res | Number | Approximate number of seconds between closing one ledger version and closing the next one. |
| accounts\_hash | String - Hash | Hash of the account information contained in this ledger, as hex. |
| transactions\_hash | String - Hash | Hash of the transaction information contained in this ledger, as hex. |
| close_time | Unsigned Integer - UNIX time | The time at which this ledger was closed. |
| close\_time\_human | String - IS0 8601 UTC Timestamp | The time at which this ledger was closed. |



# API Reference #

## Get Account Transaction History ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/8dc88fc5a9de5f6bd12dd3589b586872fe283ad3/api/routes/accountTx.js "Source")

Retrieve a history of transactions that affected a specific account. This includes all transactions the account sent, payments the account received, and payments that rippled through the account.

#### Request Format ####

<!-- <div class='multicode'> -->

*REST*

```
GET /v1/accounts/{:address}/transactions
```

<!-- </div> -->

The following URL parameters are required by this API endpoint:

| Field | Value | Description |
|-------|-------|-------------|
| address | String | The Ripple address of the account |

Optionally, you can also include the following query parameters:

| Field | Value | Description |
|-------|-------|-------------|
| type  | Single transaction type or comma-separated list of types | Filter results to include only transactions with the specified transaction type or types. Valid types include `OfferCreate`, `OfferCancel`, `Payment`, `TrustSet`, `AccountSet`, and `TicketCreate`. |
| result | Transaction result code | Filter results to only transactions with the specified transaction result code. Valid result codes include `tesSUCCESS` and all [tec codes](https://ripple.com/build/transactions#tec-codes). |
| start | ISO 8601 UTC timestamp (YYYY-MM-DDThh:mmZ) | Only retrieve transactions occurring on or after the specified date and time. |
| end   | ISO 8601 UTC timestamp (YYYY-MM-DDThh:mmZ) | Only retrieve transactions occurring on or before the specified date and time. |
| ledger_min | Integer | Sequence number of the earliest ledger to search. |
| ledger_max | Integer | Sequence number of the most recent ledger to search. |
| limit | Integer | Number of transactions to return. Defaults to 20. |
| offset | Integer | Start getting results after skipping this many. Used for paginating results. |
| descending | Boolean | Whether to order transactions with the most recent first. Defaults to true. |
| binary | Boolean | If true, retrieve transactions as hex-formatted binary blobs instead of parsed JSON objects. Defaults to false. |

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count  | Integer | The number of objects contained in the `transactions` field. |
| transactions | Array of [transaction objects](#transaction-objects) | All transactions matching the request. |


#### Example ####

Request:

```
GET /v1/accounts/r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59/transactions?limit=2&type=OfferCreate
```

Response:

```js
200 OK
{
    "result": "success",
    "count": 2,
    "transactions": [
        {
            "tx": {
                "TransactionType": "OfferCreate",
                "Flags": 131072,
                "Sequence": 159244,
                "TakerPays": {
                    "value": "0.001567373",
                    "currency": "BTC",
                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                },
                "TakerGets": "146348921",
                "Fee": "64",
                "SigningPubKey": "02279DDA900BC53575FC5DFA217113A5B21C1ACB2BB2AEFDD60EA478A074E9E264",
                "TxnSignature": "3045022100D81FFECC36A3DEF0922EB5D16F1AA5AA0804C30A18ED3B512093A75E87C81AD602206B221E22A4E3158785C109E7508624AD3DE5C0E06108D34FA709FCC9575C9441",
                "Account": "r2d2iZiCcJmNL6vhUGFjs8U8BuUq6BnmT",
                "hash": "03EDF724397D2DEE70E49D512AECD619E9EA536BE6CFD48ED167AE2596055C9A",
                "ledger_index": 8317037,
                "executed_time": 1408047740,
                "date": 461362940
            },
            "meta": {
                "TransactionIndex": 0,
                "AffectedNodes": [
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "AccountRoot",
                            "PreviousTxnLgrSeq": 8317036,
                            "PreviousTxnID": "A56793D47925BED682BFF754806121E3C0281E63C24B62ADF7078EF86CC2AA53",
                            "LedgerIndex": "2880A9B4FB90A306B576C2D532BFE390AB3904642647DCF739492AA244EF46D1",
                            "PreviousFields": {
                                "Balance": "275716601760"
                            },
                            "FinalFields": {
                                "Flags": 0,
                                "Sequence": 326323,
                                "OwnerCount": 27,
                                "Balance": "275862935331",
                                "Account": "rfCFLzNJYvvnoGHWQYACmJpTgkLUaugLEw",
                                "RegularKey": "rfYqosNivHQFJ6KpArouxoci3QE3huKNYe"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "AccountRoot",
                            "PreviousTxnLgrSeq": 8317030,
                            "PreviousTxnID": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                            "LedgerIndex": "4F83A2CF7E70F77F79A307E6A472BFC2585B806A70833CCD1C26105BAE0D6E05",
                            "PreviousFields": {
                                "OwnerCount": 18,
                                "Balance": "213169802117"
                            },
                            "FinalFields": {
                                "Flags": 0,
                                "Sequence": 1405,
                                "OwnerCount": 17,
                                "Balance": "213169802799",
                                "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "Offer",
                            "PreviousTxnLgrSeq": 8317036,
                            "PreviousTxnID": "B7256723E74A38A17C9F4F5CB938BD45581C57767E44BF26DAC8BA9FF2D58021",
                            "LedgerIndex": "55D0954D7C190006BF905D3B3D269A07016CB9F97B1E37ABE7CC8275DBBDE3DA",
                            "PreviousFields": {
                                "TakerPays": "8016312417",
                                "TakerGets": {
                                    "value": "0.085862",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                }
                            },
                            "FinalFields": {
                                "Flags": 0,
                                "Sequence": 326320,
                                "Expiration": 461364125,
                                "BookNode": "0",
                                "OwnerNode": "3",
                                "BookDirectory": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F212B4AE944DDE4",
                                "TakerPays": "7869978846",
                                "TakerGets": {
                                    "value": "0.084294634308",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "Account": "rfCFLzNJYvvnoGHWQYACmJpTgkLUaugLEw"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "RippleState",
                            "PreviousTxnLgrSeq": 8317014,
                            "PreviousTxnID": "AB356934159ECD02D27089C0AA97EF2B3DA4B3A5A3A9EC522474CADDF276B307",
                            "LedgerIndex": "5DC222A1BAF75AE489F9C78F772AEF6AF00C66660B4C28D93DC05FF4A4124D27",
                            "PreviousFields": {
                                "Balance": {
                                    "value": "-2.254933867206176",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                }
                            },
                            "FinalFields": {
                                "Flags": 2228224,
                                "LowNode": "221",
                                "HighNode": "0",
                                "Balance": {
                                    "value": "-2.253363366782792",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                },
                                "LowLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "HighLimit": {
                                    "value": "1000",
                                    "currency": "BTC",
                                    "issuer": "rfCFLzNJYvvnoGHWQYACmJpTgkLUaugLEw"
                                }
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "RippleState",
                            "PreviousTxnLgrSeq": 8317030,
                            "PreviousTxnID": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                            "LedgerIndex": "767C12AF647CDF5FEB9019B37018748A79C50EDAF87E8D4C7F39F78AA7CA9765",
                            "PreviousFields": {
                                "Balance": {
                                    "value": "-0.000000007322616",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                }
                            },
                            "FinalFields": {
                                "Flags": 131072,
                                "LowNode": "43",
                                "HighNode": "0",
                                "Balance": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                },
                                "LowLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "HighLimit": {
                                    "value": "3",
                                    "currency": "BTC",
                                    "issuer": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                                }
                            }
                        }
                    },
                    {
                        "DeletedNode": {
                            "LedgerEntryType": "DirectoryNode",
                            "LedgerIndex": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                            "FinalFields": {
                                "Flags": 0,
                                "ExchangeRate": "5F211CEE1E0697A0",
                                "RootIndex": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                                "TakerPaysCurrency": "0000000000000000000000000000000000000000",
                                "TakerPaysIssuer": "0000000000000000000000000000000000000000",
                                "TakerGetsCurrency": "0000000000000000000000004254430000000000",
                                "TakerGetsIssuer": "0A20B3C85F482532A9578DBB3950B85CA06594D1"
                            }
                        }
                    },
                    {
                        "DeletedNode": {
                            "LedgerEntryType": "Offer",
                            "LedgerIndex": "AF3C702057C9C47DB9E809FD8C76CD22521012C5CC7AE95D914EC9E226F1D7E5",
                            "PreviousFields": {
                                "TakerPays": "182677152",
                                "TakerGets": {
                                    "value": "0.001959953669835",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                }
                            },
                            "FinalFields": {
                                "Flags": 131072,
                                "Sequence": 1404,
                                "PreviousTxnLgrSeq": 8317030,
                                "BookNode": "0",
                                "OwnerNode": "0",
                                "PreviousTxnID": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                                "BookDirectory": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                                "TakerPays": "182676470",
                                "TakerGets": {
                                    "value": "0.001959946361835",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "AccountRoot",
                            "PreviousTxnLgrSeq": 8317036,
                            "PreviousTxnID": "049D5872010AE9144F8FE6A8A92937B6A21C065C2FA9A0233C9CAB7F5485ADC5",
                            "LedgerIndex": "B4C12A5134DCFE012CCC035F62D5903179DE5CABE74B228D84C7E4905BECC032",
                            "PreviousFields": {
                                "Sequence": 159244,
                                "Balance": "201007563226"
                            },
                            "FinalFields": {
                                "Flags": 0,
                                "Sequence": 159245,
                                "OwnerCount": 19,
                                "Balance": "200861228909",
                                "Account": "r2d2iZiCcJmNL6vhUGFjs8U8BuUq6BnmT"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "RippleState",
                            "PreviousTxnLgrSeq": 8317036,
                            "PreviousTxnID": "03229A664FF41A8767818DC8B236E59279078BB33C440996CB0A0093F213C173",
                            "LedgerIndex": "F16445094C00CD3A38CE7851BD6ECA77CA2F281019BE800AB984CDF57D2F2631",
                            "PreviousFields": {
                                "Balance": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                }
                            },
                            "FinalFields": {
                                "Flags": 1114112,
                                "LowNode": "0",
                                "HighNode": "233",
                                "Balance": {
                                    "value": "0.001567373",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                },
                                "LowLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "r2d2iZiCcJmNL6vhUGFjs8U8BuUq6BnmT"
                                },
                                "HighLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                }
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "DirectoryNode",
                            "LedgerIndex": "F60ADF645E78B69857D2E4AEC8B7742FEABC8431BD8611D099B428C3E816DF93",
                            "FinalFields": {
                                "Flags": 0,
                                "RootIndex": "F60ADF645E78B69857D2E4AEC8B7742FEABC8431BD8611D099B428C3E816DF93",
                                "Owner": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    }
                ],
                "TransactionResult": "tesSUCCESS"
            }
        },
        {
            "tx": {
                "TransactionType": "OfferCreate",
                "Flags": 0,
                "Sequence": 236535,
                "LastLedgerSequence": 8317038,
                "TakerPays": {
                    "value": "0.39545282",
                    "currency": "BTC",
                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                },
                "TakerGets": "36865488056",
                "Fee": "20",
                "SigningPubKey": "0382A086DB113581E08E439546156D7F34B68F11D70914B65F63A98A36AF9845DC",
                "TxnSignature": "3045022100AB584255CDA4500BD82B5EA7CBB5ABB706DF657976F79187985209DDEB05C6290220365B67073797D3D4265B6E28ACC7DB7CE4EF3DF9DDB9B62ABE8078F0BF039414",
                "Account": "rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr",
                "hash": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                "ledger_index": 8317030,
                "executed_time": 1408047700,
                "date": 461362900
            },
            "meta": {
                "TransactionIndex": 7,
                "AffectedNodes": [
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "AccountRoot",
                            "PreviousTxnLgrSeq": 8317030,
                            "PreviousTxnID": "F49ED1ACC968759EF5DA9745B08A4E47D5EA44E614AE61EE1B3CED50E1218C17",
                            "LedgerIndex": "2B9A35D51B6BABE4F386AA3C36866BDE72E14BFC917A95C6B81F5B079F1656A3",
                            "PreviousFields": {
                                "Sequence": 236535,
                                "Balance": "1035793050836"
                            },
                            "FinalFields": {
                                "Flags": 0,
                                "Sequence": 236536,
                                "OwnerCount": 30,
                                "Balance": "998934936914",
                                "Account": "rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "AccountRoot",
                            "PreviousTxnLgrSeq": 8317027,
                            "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                            "LedgerIndex": "4F83A2CF7E70F77F79A307E6A472BFC2585B806A70833CCD1C26105BAE0D6E05",
                            "PreviousFields": {
                                "Balance": "176311688215"
                            },
                            "FinalFields": {
                                "Flags": 0,
                                "Sequence": 1405,
                                "OwnerCount": 18,
                                "Balance": "213169802117",
                                "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "RippleState",
                            "PreviousTxnLgrSeq": 8317027,
                            "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                            "LedgerIndex": "767C12AF647CDF5FEB9019B37018748A79C50EDAF87E8D4C7F39F78AA7CA9765",
                            "PreviousFields": {
                                "Balance": {
                                    "value": "-0.396243732962616",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                }
                            },
                            "FinalFields": {
                                "Flags": 131072,
                                "LowNode": "43",
                                "HighNode": "0",
                                "Balance": {
                                    "value": "-0.000000007322616",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                },
                                "LowLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "HighLimit": {
                                    "value": "3",
                                    "currency": "BTC",
                                    "issuer": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                                }
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "Offer",
                            "PreviousTxnLgrSeq": 8317027,
                            "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                            "LedgerIndex": "AF3C702057C9C47DB9E809FD8C76CD22521012C5CC7AE95D914EC9E226F1D7E5",
                            "PreviousFields": {
                                "TakerPays": "37040791054",
                                "TakerGets": {
                                    "value": "0.397412773669835",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                }
                            },
                            "FinalFields": {
                                "Flags": 131072,
                                "Sequence": 1404,
                                "BookNode": "0",
                                "OwnerNode": "0",
                                "BookDirectory": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                                "TakerPays": "182677152",
                                "TakerGets": {
                                    "value": "0.001959953669835",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    },
                    {
                        "ModifiedNode": {
                            "LedgerEntryType": "RippleState",
                            "PreviousTxnLgrSeq": 8317027,
                            "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                            "LedgerIndex": "CA152527E0627E8BB24C80D0D7D9CC515376D4D5416437F7C3687CA506901D83",
                            "PreviousFields": {
                                "Balance": {
                                    "value": "-8.454143459263425",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                }
                            },
                            "FinalFields": {
                                "Flags": 2228224,
                                "LowNode": "230",
                                "HighNode": "0",
                                "Balance": {
                                    "value": "-8.849596279263425",
                                    "currency": "BTC",
                                    "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                                },
                                "LowLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                                },
                                "HighLimit": {
                                    "value": "0",
                                    "currency": "BTC",
                                    "issuer": "rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr"
                                }
                            }
                        }
                    }
                ],
                "TransactionResult": "tesSUCCESS"
            }
        }
    ]
}
```

## Get Transaction By Account and Sequence ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routes/accountTxSeq.js "Source")

Retrieve a specific transaction, by account and sequence number.

#### Request Format ####

<!-- <div class='multicode'> -->

*REST*

```
GET /v1/accounts/{:address}/transactions/{:sequence}
```

<!-- </div> -->

The following URL parameters are required by this API endpoint:

| Field | Value | Description |
|-------|-------|-------------|
| address | String | The Ripple address of the account |
| sequence | Unsigned Integer | The [sequence number of the transaction](https://ripple.com/build/transactions#common-fields). |

Optionally, you can also include the following query parameter:

| Field | Value | Description |
|-------|-------|-------------|
| binary | Boolean | If true, retrieve the transaction as a hex-formatted binary blob instead of parsed JSON objects. Defaults to false. |

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| transaction | [Transaction object](#transaction-objects) | The requested transaction |


#### Example ####

Request:
```
GET /v1/accounts/rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr/transactions/236535
```

Response:

```js
200 OK
{
    "result": "success",
    "transaction": {
        "hash": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
        "ledger_index": 8317030,
        "date": "2014-08-14T20:21:40+00:00",
        "tx": {
            "TransactionType": "OfferCreate",
            "Flags": 0,
            "Sequence": 236535,
            "LastLedgerSequence": 8317038,
            "TakerPays": {
                "value": "0.39545282",
                "currency": "BTC",
                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
            },
            "TakerGets": "36865488056",
            "Fee": "20",
            "SigningPubKey": "0382A086DB113581E08E439546156D7F34B68F11D70914B65F63A98A36AF9845DC",
            "TxnSignature": "3045022100AB584255CDA4500BD82B5EA7CBB5ABB706DF657976F79187985209DDEB05C6290220365B67073797D3D4265B6E28ACC7DB7CE4EF3DF9DDB9B62ABE8078F0BF039414",
            "Account": "rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr"
        },
        "meta": {
            "TransactionIndex": 7,
            "AffectedNodes": [
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "AccountRoot",
                        "PreviousTxnLgrSeq": 8317030,
                        "PreviousTxnID": "F49ED1ACC968759EF5DA9745B08A4E47D5EA44E614AE61EE1B3CED50E1218C17",
                        "LedgerIndex": "2B9A35D51B6BABE4F386AA3C36866BDE72E14BFC917A95C6B81F5B079F1656A3",
                        "PreviousFields": {
                            "Sequence": 236535,
                            "Balance": "1035793050836"
                        },
                        "FinalFields": {
                            "Flags": 0,
                            "Sequence": 236536,
                            "OwnerCount": 30,
                            "Balance": "998934936914",
                            "Account": "rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "AccountRoot",
                        "PreviousTxnLgrSeq": 8317027,
                        "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                        "LedgerIndex": "4F83A2CF7E70F77F79A307E6A472BFC2585B806A70833CCD1C26105BAE0D6E05",
                        "PreviousFields": {
                            "Balance": "176311688215"
                        },
                        "FinalFields": {
                            "Flags": 0,
                            "Sequence": 1405,
                            "OwnerCount": 18,
                            "Balance": "213169802117",
                            "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "RippleState",
                        "PreviousTxnLgrSeq": 8317027,
                        "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                        "LedgerIndex": "767C12AF647CDF5FEB9019B37018748A79C50EDAF87E8D4C7F39F78AA7CA9765",
                        "PreviousFields": {
                            "Balance": {
                                "value": "-0.396243732962616",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            }
                        },
                        "FinalFields": {
                            "Flags": 131072,
                            "LowNode": "0000000000000043",
                            "HighNode": "0000000000000000",
                            "Balance": {
                                "value": "-0.000000007322616",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            },
                            "LowLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "HighLimit": {
                                "value": "3",
                                "currency": "BTC",
                                "issuer": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "Offer",
                        "PreviousTxnLgrSeq": 8317027,
                        "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                        "LedgerIndex": "AF3C702057C9C47DB9E809FD8C76CD22521012C5CC7AE95D914EC9E226F1D7E5",
                        "PreviousFields": {
                            "TakerPays": "37040791054",
                            "TakerGets": {
                                "value": "0.397412773669835",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            }
                        },
                        "FinalFields": {
                            "Flags": 131072,
                            "Sequence": 1404,
                            "BookNode": "0000000000000000",
                            "OwnerNode": "0000000000000000",
                            "BookDirectory": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                            "TakerPays": "182677152",
                            "TakerGets": {
                                "value": "0.001959953669835",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "RippleState",
                        "PreviousTxnLgrSeq": 8317027,
                        "PreviousTxnID": "D92EBF936C4DEB2B99B7194C98F78A42B998E0B2C357400F139A3DA717AF8737",
                        "LedgerIndex": "CA152527E0627E8BB24C80D0D7D9CC515376D4D5416437F7C3687CA506901D83",
                        "PreviousFields": {
                            "Balance": {
                                "value": "-8.454143459263425",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            }
                        },
                        "FinalFields": {
                            "Flags": 2228224,
                            "LowNode": "0000000000000230",
                            "HighNode": "0000000000000000",
                            "Balance": {
                                "value": "-8.849596279263425",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            },
                            "LowLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "HighLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rJnZ4YHCUsHvQu7R6mZohevKJDHFzVD6Zr"
                            }
                        }
                    }
                }
            ],
            "TransactionResult": "tesSUCCESS"
        }
    }
}
```


## Get Ledger ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/6e4d601d18e60582754c8e0bde592d888cae5efc/api/routes/getLedger.js "Source")

Retrieve a specific ledger version.

#### Request Format ####

<!-- <div class='multicode'> -->

*REST*

```
GET /v1/ledgers/{:ledger_identifier}
```

<!-- </div> -->

The following URL parameters are required by this API endpoint:

| Field | Value | Description |
|-------|-------|-------------|
| ledger_identifier | Ledger Hash, Sequence Number, or ISO 8601 UTC timestamp (YYYY-MM-DDThh:mmZ) | (Optional) An identifier for the ledger to retrieve: either the full hash in hex, an integer sequence number, or a date-time. If a date-time is provided, retrieve the ledger that was most recently closed at that time. If omitted, retrieve the latest validated ledger. |

Optionally, you can also include the following query parameters:

| Field | Value | Description |
|-------|-------|-------------|
| transactions | Boolean | If `true`, include the identifying hashes of all transactions that are part of this ledger. |
| binary | Boolean | If `true`, include all transactions from this ledger as hex-formatted binary data. (If provided, overrides `transactions`.) |
| expand | Boolean | If `true`, include all transactions from this ledger as nested JSON objects. (If provided, overrides `binary` and `transactions`.) |

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| ledger | [Ledger object](#ledger-objects) | The requested ledger |

#### Example ####

Request:

```
GET /v1/ledgers/3170DA37CE2B7F045F889594CBC323D88686D2E90E8FFD2BBCD9BAD12E416DB5
```

Response:

```
200 OK
{
    "result": "success",
    "ledger": {
        "ledger_hash": "3170da37ce2b7f045f889594cbc323d88686d2e90e8ffd2bbcd9bad12e416db5",
        "ledger_index": 8317037,
        "parent_hash": "aff6e04f07f441abc6b4133f8c50c65935b817a85b895f06dba098b3fbc1be90",
        "total_coins": 99999980165594400,
        "close_time_res": 10,
        "accounts_hash": "8ad73e49a34d8b9c31bc13b8a97c56981e45ee70225ef4892e8b198fec5a1f7d",
        "transactions_hash": "33e0b9c5fd7766343e67854aed4222f5ed9c9507e0ec0d7ae7d54d0f17adb98e",
        "close_time": 1408047740,
        "close_time_human": "2014-08-14T20:22:20+00:00"
    }
}
```


## Get Transaction ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/24595d37551687aa65209369377d15a803ac8f73/api/routes/getTx.js "Source")

Retrieve a specific transaction by its identifying hash.

#### Request Format ####

<!-- <div class='multicode'> -->

*REST*

```
GET /v1/transactions/{:hash}
```

<!-- </div> -->

The following URL parameters are required by this API endpoint:

| Field | Value | Description |
|-------|-------|-------------|
| hash  | Transaction Hash | The identifying (SHA-512) hash of the transaction, as hex. |

Optionally, you can also include the following query parameters:

| Field | Value | Description |
|-------|-------|-------------|
| binary | Boolean | If `true`, return transaction data in binary format, as a hex string. Otherwise, return transaction data as nested JSON. Defaults to false. |

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| transaction | [Transaction object](#transaction-objects) | The requested transaction |

#### Example ####

Request:

```
GET /v1/transactions/03EDF724397D2DEE70E49D512AECD619E9EA536BE6CFD48ED167AE2596055C9A
```

Response:

```js
200 OK
{
    "result": "success",
    "transaction": {
        "ledger_index": 8317037,
        "date": "2014-08-14T20:22:20+00:00",
        "hash": "03EDF724397D2DEE70E49D512AECD619E9EA536BE6CFD48ED167AE2596055C9A",
        "tx": {
            "TransactionType": "OfferCreate",
            "Flags": 131072,
            "Sequence": 159244,
            "TakerPays": {
                "value": "0.001567373",
                "currency": "BTC",
                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
            },
            "TakerGets": "146348921",
            "Fee": "64",
            "SigningPubKey": "02279DDA900BC53575FC5DFA217113A5B21C1ACB2BB2AEFDD60EA478A074E9E264",
            "TxnSignature": "3045022100D81FFECC36A3DEF0922EB5D16F1AA5AA0804C30A18ED3B512093A75E87C81AD602206B221E22A4E3158785C109E7508624AD3DE5C0E06108D34FA709FCC9575C9441",
            "Account": "r2d2iZiCcJmNL6vhUGFjs8U8BuUq6BnmT"
        },
        "meta": {
            "TransactionIndex": 0,
            "AffectedNodes": [
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "AccountRoot",
                        "PreviousTxnLgrSeq": 8317036,
                        "PreviousTxnID": "A56793D47925BED682BFF754806121E3C0281E63C24B62ADF7078EF86CC2AA53",
                        "LedgerIndex": "2880A9B4FB90A306B576C2D532BFE390AB3904642647DCF739492AA244EF46D1",
                        "PreviousFields": {
                            "Balance": "275716601760"
                        },
                        "FinalFields": {
                            "Flags": 0,
                            "Sequence": 326323,
                            "OwnerCount": 27,
                            "Balance": "275862935331",
                            "Account": "rfCFLzNJYvvnoGHWQYACmJpTgkLUaugLEw",
                            "RegularKey": "rfYqosNivHQFJ6KpArouxoci3QE3huKNYe"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "AccountRoot",
                        "PreviousTxnLgrSeq": 8317030,
                        "PreviousTxnID": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                        "LedgerIndex": "4F83A2CF7E70F77F79A307E6A472BFC2585B806A70833CCD1C26105BAE0D6E05",
                        "PreviousFields": {
                            "OwnerCount": 18,
                            "Balance": "213169802117"
                        },
                        "FinalFields": {
                            "Flags": 0,
                            "Sequence": 1405,
                            "OwnerCount": 17,
                            "Balance": "213169802799",
                            "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "Offer",
                        "PreviousTxnLgrSeq": 8317036,
                        "PreviousTxnID": "B7256723E74A38A17C9F4F5CB938BD45581C57767E44BF26DAC8BA9FF2D58021",
                        "LedgerIndex": "55D0954D7C190006BF905D3B3D269A07016CB9F97B1E37ABE7CC8275DBBDE3DA",
                        "PreviousFields": {
                            "TakerPays": "8016312417",
                            "TakerGets": {
                                "value": "0.085862",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            }
                        },
                        "FinalFields": {
                            "Flags": 0,
                            "Sequence": 326320,
                            "Expiration": 461364125,
                            "BookNode": "0000000000000000",
                            "OwnerNode": "0000000000000003",
                            "BookDirectory": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F212B4AE944DDE4",
                            "TakerPays": "7869978846",
                            "TakerGets": {
                                "value": "0.084294634308",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "Account": "rfCFLzNJYvvnoGHWQYACmJpTgkLUaugLEw"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "RippleState",
                        "PreviousTxnLgrSeq": 8317014,
                        "PreviousTxnID": "AB356934159ECD02D27089C0AA97EF2B3DA4B3A5A3A9EC522474CADDF276B307",
                        "LedgerIndex": "5DC222A1BAF75AE489F9C78F772AEF6AF00C66660B4C28D93DC05FF4A4124D27",
                        "PreviousFields": {
                            "Balance": {
                                "value": "-2.254933867206176",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            }
                        },
                        "FinalFields": {
                            "Flags": 2228224,
                            "LowNode": "0000000000000221",
                            "HighNode": "0000000000000000",
                            "Balance": {
                                "value": "-2.253363366782792",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            },
                            "LowLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "HighLimit": {
                                "value": "1000",
                                "currency": "BTC",
                                "issuer": "rfCFLzNJYvvnoGHWQYACmJpTgkLUaugLEw"
                            }
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "RippleState",
                        "PreviousTxnLgrSeq": 8317030,
                        "PreviousTxnID": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                        "LedgerIndex": "767C12AF647CDF5FEB9019B37018748A79C50EDAF87E8D4C7F39F78AA7CA9765",
                        "PreviousFields": {
                            "Balance": {
                                "value": "-0.000000007322616",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            }
                        },
                        "FinalFields": {
                            "Flags": 131072,
                            "LowNode": "0000000000000043",
                            "HighNode": "0000000000000000",
                            "Balance": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            },
                            "LowLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "HighLimit": {
                                "value": "3",
                                "currency": "BTC",
                                "issuer": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                            }
                        }
                    }
                },
                {
                    "DeletedNode": {
                        "LedgerEntryType": "DirectoryNode",
                        "LedgerIndex": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                        "FinalFields": {
                            "Flags": 0,
                            "ExchangeRate": "5F211CEE1E0697A0",
                            "RootIndex": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                            "TakerPaysCurrency": "0000000000000000000000000000000000000000",
                            "TakerPaysIssuer": "0000000000000000000000000000000000000000",
                            "TakerGetsCurrency": "0000000000000000000000004254430000000000",
                            "TakerGetsIssuer": "0A20B3C85F482532A9578DBB3950B85CA06594D1"
                        }
                    }
                },
                {
                    "DeletedNode": {
                        "LedgerEntryType": "Offer",
                        "LedgerIndex": "AF3C702057C9C47DB9E809FD8C76CD22521012C5CC7AE95D914EC9E226F1D7E5",
                        "PreviousFields": {
                            "TakerPays": "182677152",
                            "TakerGets": {
                                "value": "0.001959953669835",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            }
                        },
                        "FinalFields": {
                            "Flags": 131072,
                            "Sequence": 1404,
                            "PreviousTxnLgrSeq": 8317030,
                            "BookNode": "0000000000000000",
                            "OwnerNode": "0000000000000000",
                            "PreviousTxnID": "F8E33A40A481F037BA788231421737AF2AD13161928B15A14F6ABC5007D6A2B7",
                            "BookDirectory": "7B73A610A009249B0CC0D4311E8BA7927B5A34D86634581C5F211CEE1E0697A0",
                            "TakerPays": "182676470",
                            "TakerGets": {
                                "value": "0.001959946361835",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            },
                            "Account": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "AccountRoot",
                        "PreviousTxnLgrSeq": 8317036,
                        "PreviousTxnID": "049D5872010AE9144F8FE6A8A92937B6A21C065C2FA9A0233C9CAB7F5485ADC5",
                        "LedgerIndex": "B4C12A5134DCFE012CCC035F62D5903179DE5CABE74B228D84C7E4905BECC032",
                        "PreviousFields": {
                            "Sequence": 159244,
                            "Balance": "201007563226"
                        },
                        "FinalFields": {
                            "Flags": 0,
                            "Sequence": 159245,
                            "OwnerCount": 19,
                            "Balance": "200861228909",
                            "Account": "r2d2iZiCcJmNL6vhUGFjs8U8BuUq6BnmT"
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "RippleState",
                        "PreviousTxnLgrSeq": 8317036,
                        "PreviousTxnID": "03229A664FF41A8767818DC8B236E59279078BB33C440996CB0A0093F213C173",
                        "LedgerIndex": "F16445094C00CD3A38CE7851BD6ECA77CA2F281019BE800AB984CDF57D2F2631",
                        "PreviousFields": {
                            "Balance": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            }
                        },
                        "FinalFields": {
                            "Flags": 1114112,
                            "LowNode": "0000000000000000",
                            "HighNode": "0000000000000233",
                            "Balance": {
                                "value": "0.001567373",
                                "currency": "BTC",
                                "issuer": "rrrrrrrrrrrrrrrrrrrrBZbvji"
                            },
                            "LowLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "r2d2iZiCcJmNL6vhUGFjs8U8BuUq6BnmT"
                            },
                            "HighLimit": {
                                "value": "0",
                                "currency": "BTC",
                                "issuer": "rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B"
                            }
                        }
                    }
                },
                {
                    "ModifiedNode": {
                        "LedgerEntryType": "DirectoryNode",
                        "LedgerIndex": "F60ADF645E78B69857D2E4AEC8B7742FEABC8431BD8611D099B428C3E816DF93",
                        "FinalFields": {
                            "Flags": 0,
                            "RootIndex": "F60ADF645E78B69857D2E4AEC8B7742FEABC8431BD8611D099B428C3E816DF93",
                            "Owner": "r9cZA1mLK5R5Am25ArfXFmqgNwjZgnfk59"
                        }
                    }
                }
            ],
            "TransactionResult": "tesSUCCESS"
        }
    }
}
```



## Get Ledger V2 ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getLedger.js "Source")

Retrieve a specific Ledger by hash, index, date, or latest validated.

```
GET /v2/ledgers/{:identifier}
```

#### Params ####
  * :identifier (string)...ledger hash, index, or close time.  Optional, defaults to latest close time
  * transactions (boolean)...include transaction hashes in response
  * binary (boolean)...include transactions in binary form
  * expand (boolean)...include transactions in JSON

#### Response Format ####
  same as v1





## Get Transaction V2 ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getTransactions.js "Source")

Retrieve a specific transaction by transaction hash

```
GET /v2/transactions/{:hash}
```

#### Params ####
  * :hash (string)...transaction hash of desired transaction


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:
  same as v1




## Get Transactions V2 ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getTransactions.js "Source")

Retrieve transactions by time

```
GET /v2/transactions/
```

#### Params ####
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * descending (boolean)...reverse cronological order
  * type (string)...filter transactions for a specific transaction type
  * result (string)...filter transactions for a specific transaction result
  * binary (boolean)...return transactions in binary form
  * limit (integer)...max results per page (defaults to 20)
  * marker (string)...pagination key from previously returned response

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of Transactions returned. |
| marker | String | Pagination marker |
| transactions | Array of [Transaction object](#transaction-objects) | The requested transactions |

## Get Payments ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getPayments.js "Source")

Retrieve Payments over time.  Payments are `Payment` type transactions excluding those with the same account as both the source and destination.  Results can be returned as individual payments or aggregated to a specific list of intervals if a currency and issuer is provided.

```
GET /v2/payments/{:currency}
```

#### Params ####
  * :currency(string)...base currency of the pair in the format currency[+issuer]
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)... aggregation interval (currency is required for aggregated results):
    * day
    * week
    * month
  * descending (boolean)...reverse cronological order
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of Transactions returned. |
| marker | String | Pagination marker |
| payments | Array of payments objects | The requested payments |



## Get Exchanges ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getExchanges.js "Source")

Retrieve Exchanges for a given currency pair over time.  Results can be returned as individual exchanges or aggregated to a specific list of intervals

```
GET /v2/exchanges/{:base}/{:counter}
```

#### Params ####
  * :base(string)...base currency of the pair in the format currency[+issuer] (required)
  * :counter(string)...counter currency of the pair in the format currency[+issuer] (required)
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)...aggregation interval:
    * 1minute
    * 5minute
    * 15minute
    * 30minute
    * 1hour
    * 2hour
    * 4hour
    * 1day
    * 3day
    * 7day
    * 1month
  * descending (boolean)...reverse cronological order
  * reduce (boolean)...aggregate all individual results
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * autobridged (boolean)...return only results from autobridged exchanges
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of Transactions returned. |
| marker | String | Pagination marker |
| exchanges | Array of exchange objects | The requested exchanges |



## Get Exchange Rates ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getExchangeRate.js "Source")

Retrieve an exchange rate for a given currency at a specific time.  The exchange rate is
determined by conversion first to XRP and then to the counter currency.  The rate is
derived from the volume weighted average over the calanendar day specified, averaged with
the volume weighted average of the last 50 trades within the last 14 days.  If a rate
cannot be determined, 0 is returned.

```
GET /v2/exchange_rates/{:base}/{:counter}
```

#### Params ####
  * :base(string)...base currency of the pair in the format currency[+issuer] (required)
  * :counter(string)...counter currency of the pair in the format currency[+issuer] (required)
  * date (string)...UTC date for which exchange rate is determined (defaults to now)
  * strict (boolean)...disallow rates derived from less than 10 exchanges (defaults to true)

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| rate | Number | The requested exchange rate |



## Normalize ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/normalize.js "Source")

Convert an amount from one currency/issuer denomination to another, using network
exchange rates. For non-XRP currencies, rates between each currency and XRP are used
to determine a final rate, using the same logic as the exchange rate endpoint.  If a
rate cannot be determined, 0 is used.

```
GET /v2/normalize
```

    date: smoment(req.query.date),
    amount: Number(req.query.amount),
    currency: (req.query.currency || 'XRP').toUpperCase(),
    issuer: req.query.issuer || '',
    exchange_currency: (req.query.exchange_currency || 'XRP').toUpperCase(),
    exchange_issuer: req.query.exchange_issuer || '',
    strict: (/false/i).test(req.query.strict) ? false : true

#### Params ####
  * amount (string)...amount of currency to normalize (required)
  * currency(string)...original denomination currency (defaults to XRP)
  * issuer(string)...original denomination issuer
  * exchange_currency(string)...final denomination currency (defaults to XRP)
  * exchange_issuer(string)...final denomination issuer
  * date (string)...UTC date for which exchange rate is determined (defaults to now)
  * strict (boolean)...disallow rates derived from less than 10 exchanges (defaults to true)

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| amount | Number | Original denomination amount |
| converted | Number | Final denomination amount |
| rate | Number | Exchange rate of conversion |



## Get Reports ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/reports.js "Source")

Retreive per account per day aggregated payment summaries

```
GET /v2/reports/{:date}
```

#### Params ####
  * :date (string)...UTC query date (defaults to today)
  * accounts (boolean)...include lists of counterparty accounts
  * payments (boolean)...include lists of individual payments
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| date | String | date of reports queried |
| count | Integer | Number of reports returned |
| marker | String | Pagination marker |
| reports | Array of report objects | The requested reports |



## Get Stats ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/stats.js "Source")

Retreive ripple network transaction stats

```
GET /v2/stats
GET /v2/stats/{:family}
GET /v2/stats/{:family}/{:metric}
```

#### Params ####
  * :family (string)...return only specified family ('type', 'result', or 'metric')
  * :metric (string)...return only a specific metric from the family subset
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)...aggregation interval:
    * hour
    * day
    * week
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * descending (boolean)...reverse cronological order
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of reports returned. |
| marker | String | Pagination marker |
| stats | Array of stats objects | The requested stats |


## Get Capitalization ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/capitalization.js "Source")

Get capitalization data for a specific currency + issuer

```
GET /v2/capitaliztion/{:currency+issuer}
```

#### Params ####
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)...aggregation interval
    * day
    * week
    * month
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * descending (boolean)...reverse cronological order
  * adjusted (boolean)...adjust results by removing known issuer owned wallets
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of reports returned. |
| currency | String | Currency requested |
| issuer | String | Issuer requested |
| marker | String | Pagination marker |
| rows | Array of issuer capitalization objects | The requested capitalization data |



## Get Active Accounts ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/activeAccounts.js "Source")

Get active trading accounts for a specific currency pair

```
GET /v2/active_accounts/{:base}/{:counter}
```

#### Params ####
  * :base(string)...base currency of the pair in the format currency[+issuer] (required)
  * :counter(string)...counter currency of the pair in the format currency[+issuer] (required)
  * period (string)...requested period
    * 1day
    * 3day
    * 7day
  * date (string)... start date for the selected period. If absent, the latest period is used.
  * include_exchanges(boolean)...include individual exchanges for each account in the results.
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of accounts returned. |
| exchanges_count | Integer | Total number of exchanges. |
| accounts | Array of active account objects | Active trading accounts for the period |



## Get Exchange Volume ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getMetric.js "Source")

Get aggregated exchange volume for a given time period. By default, the rolling 24 hour
data is returned. If start or end is specified, historical intervals are returned.

```
GET /v2/network/exchange_volume
```

#### Params ####
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)...aggregation interval
    * day
    * week
    * month
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of results returned. |
| rows | Array of exchange volume objects | Exchange volumes for the period selected |



## Get Payment Volume ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getMetric.js "Source")

Get aggregated payment volume for a given time period. By default, the rolling 24 hour
data is returned. If start or end is specified, historical intervals are returned.

```
GET /v2/network/payment_volume
```

#### Params ####
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)...aggregation interval
    * day
    * week
    * month
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of results returned. |
| rows | Array of payment volume objects | Payment volumes for the period selected |



## Get Issued Value ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getMetric.js "Source")

Get aggregated currency capitalization for a given time period. By default, the rolling 24 hour
data is returned. If start or end is specified, historical intervals are returned.

```
GET /v2/network/issued_value
```

#### Params ####
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of results returned. |
| rows | Array of issued value objects | Aggregated capitalization for the period selected |



## Get Accounts ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accounts.js "Source")

Get funded ripple network accounts

```
GET /v2/accounts
```

#### Params ####
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * interval (string)...aggregation interval
    * hour
    * day
    * week
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * descending (boolean)...reverse cronological order
  * parent (string)...limit results to specified parent account
  * reduce (boolean)...return a count of results only
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of reports returned. |
| marker | String | Pagination marker |
| stats | Array of account creation objects | The requested accounts |



## Get Account ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/getaccount.js "Source")

Get creation info for a specific ripple account

```
GET /v2/account/{:address}
```

#### Params ####
  * :address (string)...ripple address to query


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| account | account creation object | The requested account |



## Get Account Balances ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountBalances .js "Source")

Get balances for a specific ripple account

```
GET /v2/account/{:address}/balances
```

#### Params ####
  * :address (string)...ripple address to query
  * ledger_index (integer)...index of ledger for historical balances
  * ledger_hash (string)...ledger hash for historical balances
  * date (string)...UTC date for historical balances
  * currency(string)...restrict results to specified currency
  * issuer(string)...restrict results to specified counterparty/issuer
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'


#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| ledger_index | Integer | ledger index for balances query |
| close_time | String | close time of the ledger |
| limit | String | number of results returned, if limit was exceeded |
| marker | String | Pagination marker |
| balances | Array of balance objects | The requested balances |




## Get Account Transactions V2 ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountTransactions.js "Source")

Retrieve a history of transactions that affected a specific account. This includes all transactions the account sent, payments the account received, and payments that rippled through the account.

```
GET /v2/accounts/{:address}/transactions
```

#### Params ####
  * :address (string)...ripple address to query
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * min_sequence (string)...minimum sequence number to query
  * max_sequence (string)...max sequence number to query
  * type(string)...restrict results to a specified transaction type
  * result(string)...restrict results to specified transaction result
  * binary (boolean)..return results in binary format
  * descending (boolean)...reverse cronological order
  * limit (integer)...max results per page (defaults to 20)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count  | Integer | The number of objects contained in the `transactions` field. |
| marker | String | Pagination marker |
| transactions | Array of [transaction objects](#transaction-objects) | All transactions matching the request. |




## Get Transactions By Account And Sequence V2 ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountTxSeq.js "Source")

Retrieve a specifc transaction originating from a specified account

```
GET /v2/accounts/{:address}/transactions/{:sequence}
```

#### Params ####
  * :address (string)...ripple address to query
  * :sequence (integer)...transaction sequence
  * binary (boolean)..return transaction in binary format

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| transaction | [transaction object](#transaction-objects) | requested transaction |




## Get Account Payments ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountPayments.js "Source")

Retrieve a payments for a specified account

```
GET /v2/accounts/{:address}/payments
```

#### Params ####
  * :address (string)...ripple address to query
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * type (string)...type of payment - 'sent' or 'received'
  * currency(string)...restrict results to specified currency
  * issuer(string)...restrict results to specified issuer
  * descending (boolean)...reverse cronological order
  * limit (integer)...max results per page (defaults to 20)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####

A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count  | Integer | The number of objects contained in the `payments` field. |
| marker | String | Pagination marker |
| payments | Array of payment objects | All payments matching the request. |




## Get Account Exchanges ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountExchanges.js "Source")

Retrieve Exchanges for a given account over time.

```
GET /v2/account/{:address}/exchanges/
GET /v2/account/{:address}/exchanges/{:base}/{:counter}
```

#### Params ####
  * :address (string)...ripple address to query
  * :base(string)...base currency of the pair in the format currency[+issuer]
  * :counter(string)...counter currency of the pair in the format currency[+issuer]
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * descending (boolean)...reverse cronological order
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of exchanges returned. |
| marker | String | Pagination marker |
| exchanges | Array of exchange objects | The requested exchanges |



## Get Account Balance Changes ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountBalanceChanges.js "Source")

Retrieve Balance changes for a given account over time.

```
GET /v2/account/{:address}/balance_changes/
```

#### Params ####
  * :address (string)...ripple address to query
  * currency(string)...restrict results to specified currency
  * issuer(string)...restrict results to specified counterparty/issuer
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * descending (boolean)...reverse cronological order
  * limit (integer)...max results per page (defaults to 200)
  * marker (string)...pagination key from previously returned response
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| count | Integer | Number of balance changes returned. |
| marker | String | Pagination marker |
| exchanges | Array of balance change objects | The requested balance changes |




## Get Account Reports ##
[[Source]<br>](https://github.com/ripple/rippled-historical-database/blob/develop/api/routesV2/accountReports.js "Source")

Retrieve Daily summaries of payment activity for an account.

```
GET /v2/account/{:address}/reports/
GET /v2/account/{:address}/reports/{:date}
```

#### Params ####
  * :address (string)...ripple address to query
  * :date (string)...UTC date for single report
  * start (string)...UTC start time of query range
  * end (string)...UTC end time of query range
  * accounts (boolean)...include lists of counterparty accounts
  * payments (boolean)...include lists of individual payments
  * descending (boolean)...reverse cronological order
  * format (string)...format of returned results: 'csv','json' defaults to 'json'

#### Response Format ####
A successful response uses the HTTP code **200 OK** and has a JSON body with the following:

| Field  | Value | Description |
|--------|-------|-------------|
| result | `success` | Indicates that the body represents a successful response. |
| start | String | Start of range for reports returned |
| end | String | End of range for reports returned |
| count | Integer | Number of reports returned |
| reports | Array of report objects | The requested reports |



# Running the Historical Database #

You can also run your own instance of the Historical Database software, and populate it with transactions from your own `rippled` instance. This is useful if you do not want to depend on Ripple Labs to operate the historical database indefinitely, or you want access to historical transactions from within your own intranet.

## Installation ##

### Dependencies ###

The Historical Database requires the following software installed first:
* [PostgreSQL](http://www.postgresql.org/) (required for v1),
* [HBase](http://hbase.apache.org/) (required for v2),
* [Node.js](http://nodejs.org/)
* [npm](https://www.npmjs.org/)
* [git](http://git-scm.com/) (optional) for installation and updating.

### Installation Process ###

For v1 (postgres):
  1. Create a new postgres database:
    `createdb ripple_historical` in PostgreSQL
  2. Clone the rippled Historical Database Git Repository:
    `git clone https://github.com/ripple/rippled-historical-database.git`
    (You can also download and extract a zipped release instead.)
  3. Use npm to install additional modules:
    `cd rippled-historical-database`
    `npm install`
    The install script will also create the required config files: `config/api.config.json` and `config/import.config.json`
  4. Modify the API and import config files as needed. If you only wish to run the v1 endpoints, remove the `hbase` section from the api config.
  5. Load the latest database schema for the rippled Historical Database:
    `./node_modules/knex/lib/bin/cli.js migrate:latest`

For v2 (hbase):
  1. Set up an hbase cluster
  2. Clone the rippled Historical Database Git Repository:
    `git clone https://github.com/ripple/rippled-historical-database.git`
    (You can also download and extract a zipped release instead.)
  3. Use npm to install additional modules:
    `cd rippled-historical-database`
    `npm install`
    The install script will also create the required config files: `config/api.config.json` and `config/import.config.json`
  4. Modify the API and import config files as needed. If you only wish to run the v2 endpoints, remove the `postgres` section from the api config file.

At this point, the rippled Historical Database is installed. See [Services](#services) for the different components that you can run.

### Services ###

The `rippled` Historical Database consists of several processes that can be run separately.

* [Live Ledger Importer](#live-ledger-importer) - Monitors `rippled` for newly-validated ledgers.
    `node import/live`
* [Backfiller](#backfiller) - Populates the database with older ledgers from a `rippled` instance.
    `node import/postgres/backfill`
* API Server - Provides [REST API access](#usage) to the data.
    `npm start` (restarts the server automatically when source files change),
    or `node api/server.js` (simple start)

# Importing Data #

In order to retrieve data from the `rippled` Historical Database, you must first populate it with data. Broadly speaking, there are two ways this can happen:

* Connect to a `rippled` server that has the historical ledgers, and retrieve them. (Later, you can reconfigure the `rippled` server not to maintain history older than what you have in your Historical Database.)
    * You can choose to retrieve only new ledgers as they are validated, or you can retrieve old ledgers, too.
* Or, you can load a dump from a database that already has the historical ledger data. (At this time, there are no publicly-available database dumps of historical data.) Use the standard process for your database.

In all cases, keep in mind that the integrity of the data is only as good as the original source. If you retrieve data from a public server, you are assuming that the operator of that server is trustworthy. If you load from a database dump, you are assuming that the provider of the dump has not corrupted or tampered with the data.

## Live Ledger Importer ##

The Live Ledger Importer is a service that connects to a `rippled` server using the WebSocket API, and listens for ledger close events. Each time a new ledger is closed, the Importer requests the latest validated ledger. Although this process has some fault tolerance built in to prevent ledgers from being skipped, it is still possible that the Importer may miss ledgers.

The Live Ledger Importer includes a secondary process that runs periodically to validate the data already imported and check for gaps in the ledger history.

The Live Ledger Importer can import to one or more different data stores concurrently. If you have configured the historical database to use another storage scheme, you can use the `--type` parameter to specify the database type or types to use. If not specified, the `rippled` Historical Database defaults to PostgreSQL.

Here are some examples:

```
// defaults to Hbase:
$ node import/live

// Use Postgres instead:
$ node import/live --type postgres

// Use PostgreSQL and Hbase simultaneously:
$ node import/live --type postgres,hbase
```

## Backfiller ##

The Backfiller retrieves old ledgers from a `rippled` instance by moving backwards in time. You can optionally provide start and stop indexes to retrieve a specific range of ledgers, by their sequence number.

The `--startIndex` parameter defines the most-recent ledger to retrieve. The Backfiller retrieves this ledger first and then continues retrieving progressively older ledgers. If this parameter is omitted, the Backfiller begins with the newest validated ledger.

The `--stopIndex` parameter defines the oldest ledger to retrieve. The Backfiller stops after it retrieves this ledger. If omitted, the Backfiller continues as far back as possible. Because backfilling goes from most recent to least recent, the stop index should be a smaller than the start index.

**Warning:** The Backfiller is best for filling in relatively short histories of transactions. Importing a complete history of all Ripple transactions using the Backfiller could take weeks. If you want a full history, we recommend acquiring a database dump with early transctions, and importing it directly. Ripple Labs used the internal SQLite database from an offline `rippled` to populate its historical databases with the early transactions, then used the Backfiller to catch up to date after the import finished.

Here are some examples:

```
// retrieve everything to PostgreSQL
node import/postgres/backfill

// get ledgers #1,000,000 to #2,000,000 (inclusive) and store in hbase
node import/hbase/backfill --startIndex 2000000 --stopIndex 1000000
```


