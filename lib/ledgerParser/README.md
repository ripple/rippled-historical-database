On the Ripple Network, all modifications to the ledger occur through signed transactions.  These transactions are not necessarily financial transactions in which value moves from one account to another, but can also be account and order modifications which do not necessarily involve the movement of value. And yet every transaction will at the very least have a cost for the originating account of at least the network fee.  Furthermore, a single transaction on the ripple network can affect multiple accounts, each in different ways.  In order to determine what really happened as the result of a transaction, the transaction metadata must be parsed into a more understandable and auditable form.


###Payments
The payments table consists of transactions in which value moved from one account to another.

#####Not all 'Payments' are Payments
Though 'Payment' is a ripple transaction type, there is not a perfect 1:1 mapping from the 'Payment' transaction type.  A payment can have the same account as both the source and destination, which allows account's to utilize pathfinding to convert from one currency to another.  Because the underlying metadata affects of this sort of transaction are indistiguishable from simple exchanges, we do not parse this sort of transaction as a 'payment'.  Other than that, all successful 'Payment' transactions result in an entry in the 'payments' table.

#####Cross-currency Considerations
Payments can be both cross-currency, and cross-issuer, resulting in several complications.  One is that both the source currency and the destination currency could come from multiple different issuers.  This means that the sender, recipient, or both could have balance changes affecting multiple trust lines.  In addition to that, either the source or destination could have had open offers that were crossed in the midst of the transaction.  This makes it difficult, in edge cases, to determine who the issuer was.

#####Determining the Issuer
Our method for determining the issuer first looks at the supplied `issuer` in the transaction's `Amount` field.  If the issuer field is neither the source nor the destination address, then the transaction is specifying a particular issuer to use.  If not, it could be any issuer that the destination accepts. If thats the case we need to parse the metadata to determine the actual issuer.  We look for a `RippleState` for the payment currency in which the destination account is either the highLimit 'issuer' or lowLimit 'issuer'.  If the balance is negative, or if the previous balance was negative, the lowLimit account is the issuer, othewise the high limit account is the issuer.  This does not take into account the edge case of multiple destination issuers, but does accurately select the issuer in all other cases.

#####Delivered Amount
The actual amount of currency transferred must come from the meta data's `Delivered Amount` field - due to the partial payment flag, which can result in a successful transaction that does not send the full amount prescribed in the `Amount` field.


###Exchanges
Exchanges represent transfers of value from one denomination to another within the same account, based on offers placed in the Ripple Network's order books.

#####Identifying Exchanges
Only successful `Payment` or `OfferCreate` type transactions can result in an exchange, and a single transaction on the network can cause multiple exchanges with multiple different ripple accounts to occur.  Exchanges are identified by searching the metadata of successful `Payment` or `OfferCreate` nodes with a `LedgerEntryType` of `Offer` and contain either a modified node or a deleted node.  In this case, created nodes are ignored because they only indicate that an offer was placed into the orderbook, versus triggering an immediate exchange.  We can consider all deleted nodes to be a fully consumed order, because the transaction type was not `OfferCancel`.  Additionally, the nodes found must have `PreviousFields`, `PreviousFields.TakerGets`, and `PreviousFields.TakerPays`.

#####Interpreting the Data
Once a metadata node has been identified as indicating an exchange, The base and counter currencies are extracted from the `TakerPays` and `TakerGets` fields.  The base and counter amounts exchanged are found by subtracting the `FinalFields` values from the `PreviousFields` values.  The exchange rate is derived from the offer's `BookDirectory` field, with a fallback of dividing the counter amount by the base amount.

#####Determining the Currency Order
Depending on how the offer was created, it is possible for the base and counter currencies to be in either base/counter or counter/base ordering.  In order to consistently store the data, we chose to use the lexographical order of the currency code + issuer account, with base being the lesser of the 2 in every case.  The result is, in many cases the base and counter amounts and exchange rate get inverted.

#####Account Roles
Each offer has 2 counterparties, each of which play certain roles in the exchange.  One is a buyer, and one is a seller.  Technically they are both buyers and sellers, but we chose to look at it from the perspective of the base currency for consistency.  The buyer is the recipient of the base currency, and the seller is the on from whom it originated.  Additionally, one account placed the order passively on the exchange, while the other placed an order to consume the existing offer.  The first is considered the provider (providing liquidity) and the other is considered the taker (consumer of provided liquidity).  The one who created the transaction which caused the exchange is always the taker.  This account either placed an offer or a cross-currency payment which took advantage of the existing liquidity.  In the case of payments, it is as though the Payment initiator consumed a bunch of different offers and redistributed the proceeds to each account according to their offer, thus he is the taker and counterparty to all of them.  The provider account is found in the offer node itself, as the account that created the initial unfilled offer.

###Balance Changes
Balance changes are any change in IOU or XRP balance for a specific account.  Because all transactions on the Ripple Network at least consume a fee, it is not sufficient to look at payments and exchanges to track the value of an account.  The balance changes table combines all changes including network fees into one source, therefore it is ideal for auditing and balance reconciliation.

#####Identifying Balance Changes
All balances on the Ripple Consensus Ledger are contained in either `AccountRoot` nodes (XRP balances) or `RippleState` nodes (IOU balances)

#####Determining the Issuer
In the case of IOU balance changes, we need to determine which of the 2 accounts is the issuer.  Each `RippleState` has a `HighLimit` and a `LowLimit` - if the final balance is negative, or the previous balance was negative, then we consider the `LowLimit` issuer to be the true issuer of the balance.  We also invert the balance change amount and final balance in this case, to consistently view the balance from the non-issuer's perspective.  Otherwise, we consider the `HighLimit` issuer to be the true issuer and do not invert.

#####Separating out the network fee
Payments and OfferCreates can result in XRP balance changes from sending XRP or exchanging it, and this balance change is combined with the network fee as one balance change in the transaction metadata.  We chose to subtract the network fee from any XRP balance change, and record it separately.

###Offers

###Accounts Created
The accounts created table tracks the creation of new accounts on the Ripple Network.

#####Identifying New Accounts
Accounts are created when an existing ripple account funds a new one by sending an amount of XRP in excess of the current reserve amount to a valid public address that does not yet exist on the ripple consensus ledger.  All new accounts originate from an `Payment` transaction in XRP, and can be identified as a `AccountRoot` `CreatedNode`.  The parent of the newly created account is the account that originated the XRP payment that resulted in the account creation.

#####Genesis Ledger Accounts
Because historical data from before ledger 32570 are missing, the accounts from that ledger have been manually added to the table. It cannot be determined which account was the parent, and from which transaction they were created.
