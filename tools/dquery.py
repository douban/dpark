#!/usr/bin/env python

import os, sys
import re
import dpark
from dpark.table import TableRDD, __tables as DEFAULT_TABLE

def gen_table(exp, fields=None):
    rdd = eval('dpark.' + exp[:exp.rindex(')') + 1])
    head = rdd.first()
    if isinstance(head, str):
        if '\t' in head:
            rdd = rdd.fromCsv('excel-tab')
        elif ',' in head:
            rdd = rdd.fromCsv('excel')
        else:
            rdd = rdd.map(lambda l:l.split(' '))
        row = rdd.first()
    else:
        row = head
    if not isinstance(rdd, TableRDD):
        if not fields:
            fields = ['f%d' % i for i in range(len(row))]
        table = rdd.asTable(fields)
    else:
        table = rdd
    return table

_type_mappings = {
    'smallint': 'int',
    'varchar': 'str',
}

def get_table(name_or_expr):
    if name_or_expr in DEFAULT_TABLE:
        return DEFAULT_TABLE[name_or_expr]
    return gen_table(name_or_expr)

def create_table(check, tbl_name, cols, expr):
    if tbl_name in DEFAULT_TABLE and not check:
        print 'table %s is already exists' % tbl_name
        return
   
    if expr.lower().startswith('select'):
        sql, subtable = CMDS['select'][1].match(expr).groups()
        table = get_table(subtable).execute(sql, True)
    else:
        table = gen_table(expr)

    if cols:
        cols = [f.strip().split(' ') for f in cols[1:-1].split(',')]
        fields = [c[0] for c in cols]
        def gen_expr(i, c):
            if len(c) > 1:
                return '%s(_v[%d])' % (_type_mappings.get(c[1], c[1]), i)
            return '_v[%d]' % i
        exprs = [gen_expr(i,c) for i,c in enumerate(cols)]
        convs = eval('lambda _v: (%s,)' % (','.join(exprs)))
        table = table.prev.map(convs).asTable(fields, tbl_name)
    print 'created table', tbl_name, table
    DEFAULT_TABLE[tbl_name] = table

def show_table(pattern):
    for name in DEFAULT_TABLE:
        if not pattern or re.match(pattern, name):
            print name, DEFAULT_TABLE[name]

def drop_table(check, *names):
    for name in names:
        if name in DEFAULT_TABLE:
            del DEFAULT_TABLE[name]
        elif not check:
            print 'table %s not exists' % name

def select(sql, table_expr):
    table = get_table(table_expr)
    rs = table.execute(sql)
    if not rs: return

    width = [0] * len(rs[0])
    for row in rs:
        for i, r in enumerate(row):
            w = len(str(r))
            if w > width[i]:
                width[i] = w
    print '-' * (sum(width) + len(width) * 3 + 1)
    for row in rs:
        for i, r in enumerate(row):
            print ('| %% %ds' % width[i]) % r,
        print '|'
    print '-' * (sum(width) + len(width) * 3 + 1)

def help(topic):
    print 'supported SQL:'
    print 'CREATE TABLE [IF NOT EXISTS] tbl_name [(create_definition, ...)] rdd_expr | select_statement'
    print 'DROP TABLE [IF EXISTS] tbl_name, ...'
    print "SHOW TABLES [LIKE 'pattern']"
    print ("SELECT expr FROM tbl_name [[INNER | LEFT OUTER] JOIN tbl_name ON ] [WHERE condition] [GROUP BY cols] " +
           "[HAVING condition] [ORDER BY cols [ASC | DESC]] [LIMIT n]")

def quit():
    sys.exit(0)

CMDS = {
    'create': (create_table, re.compile(r"CREATE +TABLE +(IF +NOT +EXISTS *)? *(\w+) +(\([\w\d_, ]+\))? *(.+);?", re.I)),
    'show':   (show_table,   re.compile(r"SHOW +TABLES(?: +LIKE +'(.+?)')?;?", re.I)),
    'drop':   (drop_table,   re.compile(r"DROP TABLE +(IF +EXISTS *)?(\w+)(?:, +(\w+))*;?", re.I)),
    'select': (select,       re.compile(r"(SELECT .*? FROM +(.+?)(?: (?:(?:INNER|LEFT OUTER )?JOIN|WHERE|GROUP BY|ORDER BY|LIMIT) .+)?);?$", re.I)),
    'help':   (help,         re.compile(r'HELP( \w+)?', re.I)),
    'quit':   (quit,         re.compile(r'QUIT', re.I)),
}

REMEMBER_CMDS = ['create', 'drop']

def execute(sql):
    if sql.endswith(';'):
        sql = sql[:-1]
    cmd = sql.split(' ')[0].lower()
    if cmd in CMDS:
        func, pattern = CMDS[cmd]
        m = pattern.match(sql)
        if not m:
            print 'syntax error:', sql
        else:
            func(*m.groups())
    else:
        print 'invalid cmd: %s' % cmd

cases = [
    "help",
    "drop table if exists test",
    "create table test (f1 int, f2 int) makeRDD(zip(range(1000), range(1000)), 2)",
    "select * from test where f1 % 2 == 0 limit 10",
    "create table test2 select * from test where f1 % 2 == 0",
    "show tables test.*",
    "select sum(f2), avg(f1) from test2",
#    "drop table test, test2",
]

def test():
    for case in cases:
        execute(case)

CONF='.dquery'
def load_history():
    sysconf = os.path.join('/etc', CONF)
    if os.path.exists(sysconf):
        for line in open(sysconf):
            execute(line.strip())

    home = os.environ.get('HOME')
    if os.path.exists(os.path.join(home, CONF)):
        for line in open(os.path.join(home, CONF)):
            execute(line.strip())
    
    if os.path.exists(CONF):
        for line in open(CONF):
            execute(line.strip())

def remember(sql):
    home = os.environ.get('HOME')
    with open(os.path.join(home, CONF), 'a') as f:
        f.write(sql + '\n')
    

def shell():
    print "Welcome to DQuery 0.1, enjoy SQL and DPark! type 'help' for help."
    sql = ''
    import readline
    while True:
        try:
            sql += raw_input('>> ')
        except EOFError:
            break
        if sql not in CMDS and not sql.endswith(';'):
            continue
        try:
            execute(sql)
            if sql.split(' ')[0].lower() in ('create', 'drop'):
                remember(sql)
        except Exception, e:
            print 'ERROR:', e
            import traceback; traceback.print_exc()
        sql = ''

def main():
    from dpark import DparkContext, optParser
    #optParser.set_default('master', 'mesos')
    optParser.add_option('-e', '--query', type='string', default='',
            help='execute the SQL qeury then exit')
    
    options, args = optParser.parse_args()

    load_history()
    
    if options.query:
        execute(options.query)
        sys.exit(0)

    shell()

if __name__ == '__main__':
    #test()
    main()
