grammar UQL;

ADD:           '+';
SUB:           '-';
MUL:           '*';
DIV:           '/';
MOD:           '%'; // For "t mod '10 sec' == 0" <=> "every 10 seconds"

EQ:            '==';
NOT_EQ:        '!=';
LT:            '<';
GT:            '>';
LT_EQ:         '<=';
GT_EQ:         '>=';

LPAREN:        '(';
RPAREN:        ')';
COMMA:         ',';

AND:           'and';
OR:            'or';
NOT:           'not';

MILLISECONDS:  'milliseconds';
SECONDS:       'seconds';
MINUTES:       'minutes';
ROWS:          'rows';

ADVANCE:       'advance';
AFTER:         'after';
AGGREGATE:     'aggregate';
APPEND:        'append';
AS:            'as';
AVERAGE:       'avg';
BASED:         'based';
BEGIN:         'begin';
BY:            'by';
CHUNKING:      'chunking';
CLOCK:         'clock';
CONTINUOUSLY:  'continuously';
COUNT:         'count';
DISTINCTCOUNT: 'distinctcount';
END:           'end';
EVERY:         'every';
EXCLUSIVE:     'exclusive';
EXPIRE:        'expire';
FALSE:         'false';
FIRST:         'first';
FROM:          'from';
GROUP:         'group';
INCLUSIVE:     'inclusive';
LAST:          'last';
MAXIMUM:       'max';
MEAN:          'mean';
MINIMUM:       'min';
ON:            'on';
OF:            'of';
ORDER:         'order';
REASON:        'reason';
SESSION:       'session';
SLICE:         'slice';
SLIDE:         'slide';
SUM:           'sum';
TO:            'to';
TRUE:          'true';
UNIQUE:        'uniq';
USER:          'user';
WALL:          'wall';
WHEN:          'when';
WHERE:         'where';
WINDOW:        'window';

INTEGER:       '-'? DIGIT+;
FLOAT:         '-'? DIGIT+ ( '.' DIGIT+)? ( 'e' '-'? DIGIT+)?;
DIGIT:         [0-9];
WHITESPACE:    [ \r\n\t]+ -> skip;
NAME:          [a-zA-Z_][a-zA-Z0-9_.]*;
DQ_STRING:     '"' (~('"' | '\\' | '\r' | '\n') | '\\' ('"' | '\\'))* '"';
SQ_STRING:     '\'' (~('\'' | '\\' | '\r' | '\n') | '\\' ('\'' | '\\'))* '\'';
BOOLEAN:       FALSE | TRUE;

fieldName:      NAME;
groupName:      NAME;
projectionName: NAME;
tableName:      NAME;

start: queryClause EOF;

queryClause:
  fromClause
  groupClause?
  ingressWhereClause?
  windowClause
  aggregateClause
  aggregateWhereClause?
  appendClause
  projectWhereClause?
  toClause;

fromClause:           FROM xxx = tableName;
groupClause:          GROUP BY groups;
windowClause:         WINDOW (sliceWindow | slideWindow | sessionWindow);
aggregateClause:      AGGREGATE aggregations;
appendClause:         APPEND projections;
toClause:             TO tableName;

ingressWhereClause:   whereClause;
aggregateWhereClause: whereClause;
projectWhereClause:   whereClause;
whereClause:          WHERE expression;

expression
  : left = term op = (LT | LT_EQ | EQ | NOT_EQ | GT_EQ | GT) right = term  # Equation
  | NOT LPAREN expression RPAREN                                           # Negation
  | left = expression op = (AND | OR) right = expression                   # Connection
  ;

term
  : duration                          # IgnoreMeDuration
  | atom                              # IgnoreMeBasic
  | term op = (MUL | DIV | MOD) term  # MulDivMod
  | term op = (ADD | SUB) term        # AddSub
  | LPAREN term RPAREN                # Parenthesis
  ;

atom
  : FLOAT      # Float
  | INTEGER    # Integer
  | DQ_STRING  # String
  | SQ_STRING  # Timestamp
  | NAME       # Variable
  ;

duration: amount = INTEGER unit = (MILLISECONDS | SECONDS | MINUTES);
distance: amount = INTEGER unit = ROWS;

sliceWindow:   SLICE (duration | distance) sequenceFieldClause?;
slideWindow:   SLIDE s = duration ADVANCE EVERY a = duration;
sequenceFieldClause: BASED ON fieldName;

sessionWindow: SESSION BEGIN WHEN open = sessionOpen END WHEN close = sessionClose EXPIRE AFTER life = duration;
sessionOpen:   expression;
sessionClose:  expression clusivity = (EXCLUSIVE | INCLUSIVE);

groups:        groupName      (COMMA groupName)*;
projections:   projectionName (COMMA projectionName)*;
aggregations:  aggregation    (COMMA aggregation)*;
aggregation:   aggregate AS fieldName;

aggregate
  : AVERAGE LPAREN fieldName RPAREN        # aggregateAverage
  | COUNT LPAREN fieldName RPAREN          # aggregateCount
  | COUNT LPAREN RPAREN                    # aggregateCountWithoutAsterisk
  | DISTINCTCOUNT LPAREN fieldName RPAREN  # aggregateDistinctCount
  | FIRST LPAREN fieldName RPAREN          # aggregateFirst
  | GROUP LPAREN fieldName RPAREN          # aggregateGroup
  | LAST LPAREN fieldName RPAREN           # aggregateLast
  | MAXIMUM LPAREN fieldName RPAREN        # aggregateMaximum
  | MEAN LPAREN fieldName RPAREN           # aggregateMean
  | MINIMUM LPAREN fieldName RPAREN        # aggregateMinimum
  | SUM LPAREN fieldName RPAREN            # aggregateSum
  | UNIQUE LPAREN fieldName RPAREN         # aggregateUnique
  | REASON LPAREN RPAREN                   # aggregateReasonForWindowClose
  ;
