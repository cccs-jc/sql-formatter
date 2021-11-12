import Formatter from '../core/Formatter';
import { isEnd, isWindow } from '../core/token';
import Tokenizer from '../core/Tokenizer';
import tokenTypes from '../core/tokenTypes';

const reservedWords = [
  'ALL',
  'ALTER',
  'ANALYSE',
  'ANALYZE',
  'AS',
  'BETWEEN',
  'CASCADE',
  'CASE',
  'COLUMN',
  'COLUMNS',
  'COMMENT',
  'CONSTRAINT',
  'CONTAINS',
  'CONVERT',
  'CUME_DIST',
  'CURRENT ROW',
  'DATABASE',
  'DATABASES',
  'DAY',
  'DAYS',
  'DEFAULT',
  'DELETE',
  'DESCRIBE',
  'DISTINCT',
  'DISTINCTROW',
  'DIV',
  'DROP',
  'ELSE',
  'ENCODE',
  'END',
  'EXPLAIN',
  'EXPLODE_OUTER',
  'EXPLODE',
  'FIXED',
  'FOLLOWING',
  'FULL',
  'GREATEST',
  'HOUR_MINUTE',
  'HOUR_SECOND',
  'HOUR',
  'HOURS',
  'IF',
  'IFNULL',
  'IN',
  'INSERT',
  'INTERVAL',
  'INTO',
  'IS',
  'LEVEL',
  'MERGE',
  'MINUTE_SECOND',
  'MINUTE',
  'MONTH',
  'NATURAL',
  'NOT',
  'NOW()',
  'NTILE',
  'NULL',
  'NULLIF',
  'OFFSET',
  'ON DELETE',
  'ON UPDATE',
  'ON',
  'ONLY',
  'OPTIMIZE',
  'OVER',
  'PERCENT_RANK',
  'PRECEDING',
  'RANGE',
  'RANK',
  'RENAME',
  'ROW',
  'ROWS',
  'SECOND',
  'SEPARATOR',
  'SIZE',
  'STRING',
  'STRUCT',
  'TABLE',
  'TABLES',
  'TEMPORARY',
  'THEN',
  'TO',
  'TRAILING',
  'TRANSFORM',
  'TRUE',
  'TRUNCATE',
  'TYPE',
  'TYPES',
  'UNBOUNDED',
  'UNIQUE',
  'UNLOCK',
  'UNSIGNED',
  'USING',
  'VARIABLES',
  'VIEW',
  'WHEN',
  'WITH',
  'YEAR_MONTH',
];

const reservedTopLevelWords = [
  'ADD',
  'AFTER',
  'ALTER COLUMN',
  'ALTER DATABASE',
  'ALTER SCHEMA',
  'ALTER TABLE',
  'CLUSTER BY',
  'CLUSTERED BY',
  'DELETE FROM',
  'DISTRIBUTE BY',
  'FROM',
  'GROUP BY',
  'HAVING',
  'INSERT INTO',
  'INSERT',
  'LIMIT',
  'OPTIONS',
  'ORDER BY',
  'PARTITION BY',
  'PARTITIONED BY',
  'RANGE',
  'ROWS',
  'SELECT',
  'SET CURRENT SCHEMA',
  'SET SCHEMA',
  'SET',
  'TBLPROPERTIES',
  'UPDATE',
  'USING',
  'VALUES',
  'WHERE',
  'WINDOW',
];

const reservedTopLevelWordsNoIndent = [
  'EXCEPT ALL',
  'EXCEPT',
  'INTERSECT ALL',
  'INTERSECT',
  'UNION ALL',
  'UNION',
];

const reservedNewlineWords = [
  'AND',
  'CREATE OR',
  'CREATE',
  'ELSE',
  'LATERAL VIEW',
  'OR',
  'OUTER APPLY',
  'WHEN',
  'XOR',
  // joins
  'JOIN',
  'INNER JOIN',
  'LEFT JOIN',
  'LEFT OUTER JOIN',
  'RIGHT JOIN',
  'RIGHT OUTER JOIN',
  'FULL JOIN',
  'FULL OUTER JOIN',
  'CROSS JOIN',
  'NATURAL JOIN',
  // non-standard-joins
  'ANTI JOIN',
  'SEMI JOIN',
  'LEFT ANTI JOIN',
  'LEFT SEMI JOIN',
  'RIGHT OUTER JOIN',
  'RIGHT SEMI JOIN',
  'NATURAL ANTI JOIN',
  'NATURAL FULL OUTER JOIN',
  'NATURAL INNER JOIN',
  'NATURAL LEFT ANTI JOIN',
  'NATURAL LEFT OUTER JOIN',
  'NATURAL LEFT SEMI JOIN',
  'NATURAL OUTER JOIN',
  'NATURAL RIGHT OUTER JOIN',
  'NATURAL RIGHT SEMI JOIN',
  'NATURAL SEMI JOIN',
];

export default class SparkSqlFormatter extends Formatter {
  tokenizer() {
    return new Tokenizer({
      reservedWords,
      reservedTopLevelWords,
      reservedNewlineWords,
      reservedTopLevelWordsNoIndent,
      stringTypes: [`""`, "''", '``', '{}'],
      openParens: ['(', 'CASE'],
      closeParens: [')', 'END'],
      indexedPlaceholderTypes: ['?'],
      namedPlaceholderTypes: ['$'],
      lineCommentTypes: ['--'],
      operators: ['!=', '<=>', '&&', '||', '==', '->'],
    });
  }

  tokenOverride(token) {
    // Fix cases where names are ambiguously keywords or functions
    if (isWindow(token)) {
      const aheadToken = this.tokenLookAhead();
      if (aheadToken && aheadToken.type === tokenTypes.OPEN_PAREN) {
        // This is a function call, treat it as a reserved word
        return { type: tokenTypes.RESERVED, value: token.value };
      }
    }

    // Fix cases where names are ambiguously keywords or properties
    if (isEnd(token)) {
      const backToken = this.tokenLookBehind();
      if (backToken && backToken.type === tokenTypes.OPERATOR && backToken.value === '.') {
        // This is window().end (or similar) not CASE ... END
        return { type: tokenTypes.WORD, value: token.value };
      }
    }

    return token;
  }
}
