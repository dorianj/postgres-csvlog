const _ = require('lodash');
const csv = require('csv');
const fs = require('fs');
const should = require('should');
const toArray = require('stream-to-array');

const postgresCSVLog = require('../index');

it('should parse log lines correctly', (done) => {
  stream = fs.createReadStream('test/test_log.csv').pipe(postgresCSVLog());
  toArray(stream, (err, arr) => {
    if (err) {
      done(err);
    } else {
      _.sortBy(arr, 'log_time').should.eql([
        {
          application_name: 'psql',
          command_tag: 'SELECT',
          connection_from: '127.0.0.1:60135',
          context: '',
          database_name: 'maindb',
          detail: '',
          error_severity: 'ERROR',
          hint: '',
          internal_query: '',
          internal_query_pos: '',
          location: '',
          log_time: new Date('Sat Mar 12 2016 15:49:23.567 GMT-0800 (PST)'),
          message: 'column "nine" does not exist',
          process_id: 689,
          query: 'select nine;',
          query_pos: '8',
          session_id: '56e4aaeb.2b1',
          session_line_num: 3,
          session_start_time: new Date('Sat Mar 12 2016 15:48:59 GMT-0800 (PST)'),
          sql_state_code: '42703',
          transaction_id: '0',
          user_name: 'primaryuser',
          virtual_transaction_id: '2/12',
        },
        {
          application_name: 'psql',
          command_tag: 'SELECT',
          connection_from: '127.0.0.1:60135',
          context: '',
          database_name: 'maindb',
          detail: '',
          duration: 2.277,
          error_severity: 'LOG',
          hint: '',
          internal_query: '',
          internal_query_pos: '',
          location: '',
          log_time: new Date('Sat Mar 12 2016 15:49:51.753 GMT-0800 (PST)'),
          message: 'duration: 2.277 ms  statement: SELECT *\nFROM "pg_stat_activity";',
          process_id: 689,
          query: 'SELECT *\nFROM "pg_stat_activity";',
          query_pos: '',
          session_id: '56e4aaeb.2b1',
          session_line_num: 4,
          session_start_time: new Date('Sat Mar 12 2016 15:48:59 GMT-0800 (PST)'),
          sql_state_code: '00000',
          transaction_id: '0',
          user_name: 'primaryuser',
          virtual_transaction_id: '2/0',
        },
      ]);
      done();
    }
  });
});

