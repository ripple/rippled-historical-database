'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.createTable('control',function(table) {
      table.string('key').primary();
      table.string('value');
    })
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.schema.dropTable('control')
  ])
};
