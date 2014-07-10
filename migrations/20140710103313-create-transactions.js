module.exports = {
  up: function(migration, DataTypes, done) {
    // add altering commands here, calling 'done' when finished
    migration.createTable(
      'transactions',
      {
        id: {
          type: DataTypes.INTEGER,
          primaryKey: true,
          autoIncrement: true
        },
        type: {
          type: DataTypes.ENUM([
            'Payment',
            'OfferCreate',
            'OfferCancel',
            'AccountSet',
            'SetRegularKey',
            'TrustSet'
          ])
        },
        from_account: {
          type: DataTypes.BIGINT
        },
        from_sequence: {
          type: DataTypes.BIGINT
        },
        ledger_sequence: {
          type: DataTypes.BIGINT
        },
        hash: {
          type: DataTypes.BLOB
        },
        status: {
          type: DataTypes.CHAR
        },
        raw: {
          type: DataTypes.BLOB
        },
        meta: {
          type: DataTypes.BLOB
        },
        createdAt: {
          type: DataTypes.DATE
        },
        updatedAt: {
          type: DataTypes.DATE
        }
      },
      done
    )
  },
  down: function(migration, DataTypes, done) {
    migration.dropTable('transactions', done);
  }
}
