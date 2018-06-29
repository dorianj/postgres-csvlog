const _ = require('lodash');
const csv = require('csv');
const fs = require('fs');
const should = require('should');
const toArray = require('stream-to-array');

const postgresCSVLog = require('../index');

// Note that each test case processes the entire file despite only checking parts of the output. It's potentially worth refactoring this to
// remove the repeated processing.

it('should correctly parse error log lines', (done) => {
  stream = fs.createReadStream('test/test_log.csv').pipe(postgresCSVLog());
  toArray(stream, (err, arr) => {
    if (err) {
      done(err);
    } else {
      const errorRecord = _.sortBy(arr, 'log_time')[0];
      errorRecord.should.eql(
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
        });
      done();
    }
  });
});

it('should correctly parse log lines with auto_explain message info', (done) => {
  stream = fs.createReadStream('test/test_log.csv').pipe(postgresCSVLog());
  toArray(stream, (err, arr) => {
    if (err) {
      done(err);
    } else {
      const recordWithAutoExplainInfo = _.sortBy(arr, 'log_time')[1];
      recordWithAutoExplainInfo.should.eql(
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
          message: [
            "duration: 2.277 ms  plan:",
            "Query Text: select * from generate_series(1,1000000) g(i) natural join generate_series(1,1000000) f(i);",
            "Merge Join  (cost=119.66..199.66 rows=5000 width=4)",
            "  Merge Cond: (g.i = f.i)",
            "  ->  Sort  (cost=59.83..62.33 rows=1000 width=4)",
            "        Sort Key: g.i",
            "        ->  Function Scan on generate_series g  (cost=0.00..10.00 rows=1000 width=4)",
            "  ->  Sort  (cost=59.83..62.33 rows=1000 width=4)",
            "        Sort Key: f.i",
            "        ->  Function Scan on generate_series f  (cost=0.00..10.00 rows=1000 width=4)",
            ].join('\n'),
          process_id: 689,
          query: [
            "select * from generate_series(1,1000000) g(i) natural join generate_series(1,1000000) f(i);",
            "Merge Join  (cost=119.66..199.66 rows=5000 width=4)",
            "  Merge Cond: (g.i = f.i)",
            "  ->  Sort  (cost=59.83..62.33 rows=1000 width=4)",
            "        Sort Key: g.i",
            "        ->  Function Scan on generate_series g  (cost=0.00..10.00 rows=1000 width=4)",
            "  ->  Sort  (cost=59.83..62.33 rows=1000 width=4)",
            "        Sort Key: f.i",
            "        ->  Function Scan on generate_series f  (cost=0.00..10.00 rows=1000 width=4)",
          ].join('\n'),
          query_pos: '',
          session_id: '56e4aaeb.2b1',
          session_line_num: 4,
          session_start_time: new Date('Sat Mar 12 2016 15:48:59 GMT-0800 (PST)'),
          source: 'from_auto_explain',
          sql_state_code: '00000',
          transaction_id: '0',
          user_name: 'primaryuser',
          virtual_transaction_id: '2/0',
        });
      done();
    }
  });
});

it('should correctly parse log lines with auto_explain message info containing invalid JSON', (done) => {
  stream = fs.createReadStream('test/test_log.csv').pipe(postgresCSVLog());
  toArray(stream, (err, arr) => {
    if (err) {
      done(err);
    } else {
      const recordWithInvalidStatementInfo = _.sortBy(arr, 'log_time')[2];
      recordWithInvalidStatementInfo.should.eql(
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
          log_time: new Date('Sat Mar 12 2016 15:59:52.853 GMT-0800 (PST)'),
          message: 'duration: 2.277 ms  plan: not a valid entry.',
          process_id: 689,
          query: '',
          query_pos: '',
          session_id: '56e4aaeb.2b1',
          session_line_num: 4,
          session_start_time: new Date('Sat Mar 12 2016 15:48:59 GMT-0800 (PST)'),
          source: 'from_auto_explain',
          sql_state_code: '00000',
          transaction_id: '0',
          user_name: 'primaryuser',
          virtual_transaction_id: '2/0',
        });
      done();
    }
  });
});

it('should correctly parse log lines with log_min_duration_statement info', (done) => {
  stream = fs.createReadStream('test/test_log.csv').pipe(postgresCSVLog());
  toArray(stream, (err, arr) => {
    if (err) {
      done(err);
    } else {
      const recordWithStatementInfo = _.sortBy(arr, 'log_time')[3];
      recordWithStatementInfo.should.eql(
        {
          application_name: 'psql',
          command_tag: 'SELECT',
          connection_from: '127.0.0.1:60135',
          context: '',
          database_name: 'maindb',
          detail: '',
          duration: 3199.456,
          error_severity: 'LOG',
          hint: '',
          internal_query: '',
          internal_query_pos: '',
          location: '',
          log_time: new Date('Sat Mar 12 2016 15:59:55.753 GMT-0800 (PST)'),
          message: 'duration: 3199.456 ms  statement: select * from generate_series(1,2000000) g(i) natural join generate_series(1,2000000) f(i);',
          process_id: 42,
          query: 'select * from generate_series(1,2000000) g(i) natural join generate_series(1,2000000) f(i);',
          query_pos: '',
          session_id: '5ad52121.2a',
          session_line_num: 5,
          session_start_time: new Date('Sat Mar 12 2016 15:49:59 GMT-0800 (PST)'),
          source: 'from_log_min_duration_statement',
          sql_state_code: '00000',
          transaction_id: '0',
          user_name: 'primaryuser',
          virtual_transaction_id: '2/0',
        });
      done();
    }
  });
});
