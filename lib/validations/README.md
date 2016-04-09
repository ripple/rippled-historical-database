Validations ETL
==================

The Validations ETL is a service for recording validations observed on the ripple network.  Data is saved into hbase for querying via the API.


###Components

* Validations
 * The ETL service attempts to subscribe to validations streams from known ripple network nodes.
* Validators
 * Validators are identified through the validation_public_key, and recurring reports are generated to measure the validator's performance.
 * Daily validator reports are generated every 10 minutes
* Manifests
 * Manifests track validation public key changes (for nodes using master/ephemeral keys)
* Validator Domain Verification
 * Validator domains can be verified using data from the ripple network and ripple.txt data from the domain.
 * domain verification is preformed every hour

####Installation
`$ npm install`

####starting the service
`$ node lib/validations/etl`
