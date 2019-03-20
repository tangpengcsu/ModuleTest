grammar SqlBuild;

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;
object

   : '{' pair (','pair)*'}'

   | '{' '}' // empty object

   ;

pair: STRING ':' value;
value

   :STRING

   |NUMBER

   |object // recursion

   |array // recursion

   | 'true' // keywords

   | 'false'

   | 'null'

   ;
array

      : '[' value (','value)*']'

      | '[' ']' // empty array

      ;

identifier
    : strictIdentifier
    | ANTI | FULL | INNER | LEFT | SEMI | RIGHT | NATURAL | JOIN | CROSS | ON
    | UNION | INTERSECT | EXCEPT | SETMINUS
    ;
tableIdentifier
    : (db=identifier '.')? table=identifier
    ;
joinRelation
    : joinType JOIN
    ;
joinType
    : INNER?
    | CROSS
    | LEFT OUTER?
    | LEFT SEMI
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT? ANTI
    ;

TABLENAME:'TABLE.NAME';
TABLEALIAS:'TABLE.ALIAS';

UNION: 'UNION';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
SEMI: 'SEMI';
RIGHT: 'RIGHT';
FULL: 'FULL';

ON: 'ON';


EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';