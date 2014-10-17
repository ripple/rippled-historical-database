'use strict';

exports.up = function(knex, Promise) {
  return Promise.all([
    knex.schema.createTable('ctrl_current_state',function(table) {
      table.string('key');
      table.string('value');
    })
  ]); 
};

exports.down = function(knex, Promise) {
  return Promise.all([
    knex.schema.dropTable('ctrl_current_state')
  ])  
};
