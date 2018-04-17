"use strict";

const _ = require('lodash');
const csv = require('csv');
const multipipe = require('multipipe');
const stream = require('stream');

const _messagePattern = /^duration: (\d+\.\d+)\s*ms\s+(plan|statement):\s*([\s\S]*)$/;
const _textPattern = /^Query Text:\s*([\s\S]*)$/;
const _dateFields = ['session_start_time', 'log_time'];
const _numericFields = ['process_id', 'session_line_num'];

class PostgresCSVLog extends stream.Transform {
  constructor(options) {
    super({
      allowHalfOpen: false,
      readableObjectMode: true,
      writableObjectMode: true
    });
  }

  static extractAutoExplainMessageFields(messagePlan) {
    const textMatch = messagePlan.match(_textPattern);
    if (_.size(textMatch)) {
      return {
        query: textMatch[1]
      };
    }
    else {
      let plan;
      try {
        plan = JSON.parse(messagePlan);
      }
      catch (e) {
        return;
        // If the query/plan is not in text format or JSON format, we just
        // ignore it here since there isn't much else we can do.
      }

      return {
        plan: plan,
        query: plan['Query Text'],
      };
    }
  }

  static extractStatementMessageFields(statement) {
    return {
      query: statement
    };
  }

  _transform(record, encoding, callback) {
    for (const field of _dateFields) {
      record[field] = new Date(record[field]);
    }

    for (const field of _numericFields) {
      record[field] = +record[field];
    }

    if (record.sql_state_code === '00000') {
      const messageMatches = record.message.match(_messagePattern);
      if (!_.size(messageMatches)) {
        return;
      }

      record.duration = +messageMatches[1];

      if (messageMatches[2] === 'plan') {
        _.assign(record, PostgresCSVLog.extractAutoExplainMessageFields(messageMatches[3]));
      }
      // Parse log_min_duration_statement lines, which start with `statement` instead of `plan`.
      if (messageMatches[2] === 'statement') {
        _.assign(record, PostgresCSVLog.extractStatementMessageFields(messageMatches[3]));
      }
    }

    this.push(record);
    setImmediate(callback);
  }
}

module.exports = (options) => {
  const csvParser = csv.parse({
    columns: [
      'log_time',
      'user_name',
      'database_name',
      'process_id',
      'connection_from',
      'session_id',
      'session_line_num',
      'command_tag',
      'session_start_time',
      'virtual_transaction_id',
      'transaction_id',
      'error_severity',
      'sql_state_code',
      'message',
      'detail',
      'hint',
      'internal_query',
      'internal_query_pos',
      'context',
      'query',
      'query_pos',
      'location',
      'application_name',
    ],
  });
  const logParser = new PostgresCSVLog(options);
  return multipipe(csvParser, logParser);
};
