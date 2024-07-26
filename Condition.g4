grammar Condition;

MUL: '*';
DIV: '/';
ADD: '+';
SUB: '-';
MOD: '%';

EQ:     '==';
NOT_EQ: '!=';
LT:     '<';
GT:     '>';
LT_EQ:  '<=';
GT_EQ:  '>=';

LPAREN: '(';
RPAREN: ')';

AND:     'and';
OR:      'or';
NOT:     'not';
MINUTES: 'minutes';
SECONDS: 'seconds';
ROWS:    'rows';

INTEGER:    '-'? DIGIT+;
FLOAT:      '-'? DIGIT+ ( '.' DIGIT+)? ( 'e' '-'? DIGIT+)?;
DIGIT:      [0-9];
WHITESPACE: [ \r\n\t]+ -> skip;
NAME:       [a-zA-Z_][a-zA-Z0-9_.]*;
DQ_STRING:  '"' (~('"' | '\\' | '\r' | '\n') | '\\' ('"' | '\\'))* '"';
SQ_STRING:  '\'' (~('\'' | '\\' | '\r' | '\n') | '\\' ('\'' | '\\'))* '\'';

start: expression EOF;

expression
  : left = term op = ('<' | '<=' | '==' | '!=' | '>=' | '>') right = term  # Equation
  | NOT '(' expression ')'                                                 # Negation
  | left = expression op = (AND | OR) right = expression                   # Connection
  ;

term
  : duration                          # IgnoreMeDuration
  | term op = ('*' | '/' | '%') term  # MulDivMod
  | term op = ('+' | '-') term        # AddSub
  | atom	                          # IgnoreMeBasic
  | '(' term ')'                      # Parenthesis
  ;

duration: amount = INTEGER unit = (SECONDS | MINUTES | ROWS);

atom
  : FLOAT      # Float
  | INTEGER    # Integer
  | DQ_STRING  # String
  | SQ_STRING  # Timestamp
  | NAME       # Variable
  ;
