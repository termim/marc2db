# -*- coding: utf-8 -*-
"""
   %prog [options] file1 [file2 ...]

   %prog - это скрипт для извлечения информации из файлов в формате MARC-21
   и записи её в базу данных. Поддерживаютя SQLite, PostgreSQL и MySQL.
   В БД создаются три таблицы:
   CREATE TABLE records (
     id     INTEGER PRIMARY KEY,
     leader TEXT NOT NULL,
     c000   TEXT,
     c001   TEXT,
     c002   TEXT,
     c003   TEXT,
     c004   TEXT,
     c005   TEXT,
     c006   TEXT,
     c007   TEXT,
     c008   TEXT,
     c009   TEXT
   )
   CREATE TABLE fields (
     id          INTEGER PRIMARY KEY,
     record_id   INTEGER REFERENCES marc_records (id),
     tag CHAR(3) NOT NULL,
     indicator_1 CHAR NOT NULL DEFAULT '',
     indicator_2 CHAR NOT NULL DEFAULT ''
   )
   CREATE TABLE subfields (
     id       INTEGER PRIMARY KEY,
     field_id INTEGER REFERENCES marc_fields (id),
     code     CHAR NOT NULL,
     data     TEXT NOT NULL
   )
   Имена таблиц можно выбрать с помощью параметров.
   Примеры:

   python marc2db.py -t postgres -D booksdb "../Scriblio 2007/part01.dat"
   python marc2db.py -t sqlite -D rsl01.sqlite -o rsl01.mrc
   
   Вывести на stdout информацию об авторах, используя в качестве
   разделителя символ '|':

       %prog -s "|" -f 100_a,100_b,100_c,100_d rsl01.mrc

   То же самое, но используя алиасы:

       %prog -s "|" -f author,a_num,a_words,a_dates rsl01.mrc
"""

import sys
import os.path
from time import clock, time
from datetime import timedelta
import pymarc
from optparse import OptionParser
import logging as log


reload(sys)
sys.setdefaultencoding('utf8')
del sys.setdefaultencoding



def error(msg):
    print >> sys.stderr, msg
    sys.exit(1)



class DBWriter:

    control_fields = ("000", "001", "002", "003", "004",
                      "005", "006", "007", "008", "009")
    control_columns = [u"c" + f for f in control_fields]

    fields_order = ("records", "fields", "subfields")
    
    fields = dict(
        records = [("id",     "INTEGER PRIMARY KEY"),
                   ("leader", "TEXT NOT NULL")
                   ] + zip(control_columns, ["TEXT" for x in control_columns]),
        fields = (("id",          "INTEGER PRIMARY KEY"),
                  ("record_id",   "INTEGER REFERENCES %(records)s (id)"),
                  ("tag",         "CHAR(3) NOT NULL"),
                  ("indicator_1", "CHAR NOT NULL DEFAULT ''"),
                  ("indicator_2", "CHAR NOT NULL DEFAULT ''"),
                  ),
        subfields = (("id",       "INTEGER PRIMARY KEY"),
                     ("field_id", "INTEGER REFERENCES %(fields)s (id)"),
                     ("code",     "CHAR NOT NULL"),
                     ("data",     "TEXT NOT NULL")
                     )
        )

    table_exists_sql = None
    paramstyle = '?'

    def __init__(self, options):
        self.record_id = 0
        self.field_id = 0
        self.subfield_id = 0
        self.options = options
        self.tables = dict(
            records = options.records_table,
            fields = options.fields_table,
            subfields = options.subfields_table
            )
        self.create_sql = {}
        for table in self.tables:
            fmt = "CREATE TABLE %%%%(%s)s (%%s)" % table
            fmt %= ',\n'.join([' '.join(r) for r in self.fields[table]])
            self.create_sql[table] = fmt % self.tables
        self.insert_sql = {}
        for table in self.tables:
            fmt = "INSERT INTO %%(%s)s (%%%%s) VALUES (%%%%s)" % table % self.tables
            fields = [x[0] for x in self.fields[table]]
            params = [self.paramstyle for x in fields]
            self.insert_sql[table] = fmt % (','.join(fields),','.join(params))
        self.connect(options)
        self.create_tables(options.overwrite)

    def close(self):
        self.curs.close()
        self.conn.commit()
        self.conn.close()

    def table_exists(self, tablename):
        self.curs.execute(self.table_exists_sql, (tablename,))
        res = self.curs.fetchone()
        #print self.table_exists_sql
        
        #print 'res:',res
        if not res:
            return False
        elif res[0] == tablename:
            return True
        else:
            return int(res[0]) == 1

    def create_tables(self, overwrite):
        load_ids = False
        for table in self.fields_order:
            name = self.tables[table]
            #print name
            if self.table_exists(name):
                #print 'exists:'
                if not overwrite:
                    load_ids = True
                    continue
                #print 'drop:',"DROP TABLE %s CASCADE" % name
                self.curs.execute("DROP TABLE %s CASCADE" % name)
            #print 'create:',self.create_sql[table]
            self.curs.execute(self.create_sql[table])
        self.conn.commit()
        #print "load_ids:",load_ids
        if load_ids:
            self.load_ids()

    def load_ids(self):
        def get_max(tbl):
            sql = "SELECT max(id) FROM %s"
            self.curs.execute(sql % self.tables[tbl])
            res = self.curs.fetchone()[0]
            if res:
                return int(res)
            else:
                return 0
        self.record_id = get_max('records')
        self.field_id = get_max('fields')
        self.subfield_id = get_max('subfields')

    def append(self, record):

        #print unicode(record)
        self.record_id += 1
        fields = []
        cd = {}
        for field in record.get_fields():
            if field.tag in self.control_fields:
                #print "C: <%s>" % field.tag
                cd[field.tag] = field.data
            else:
                #print "D: <%s>" % field.tag
                fields.append(field)

        cfields = [self.record_id, record.leader]
        for cn in self.control_fields:
            cfields.append(cd.get(cn, u''))
        #print self.insert_sql['records'], cfields
        #self.curs.execute(self.insert_sql['records'], cfields)
        self.rec_lst.append(cfields)
        
        #print len(fields), fields
        #lst = []
        #lsts = []
        for field in fields:
            #print "d: <%s>" % field.tag,unicode(field)
            self.field_id += 1
            #print field
            #if isinstance(field,
            try:
                i1, i2 = field.indicator1, field.indicator2
                #print "i1 <%s>" % i1, type(i1)
                #print "i2 <%s>" % i2, type(i2)
                self.fld_lst.append((self.field_id, self.record_id, field.tag, i1, i2))
                #self.curs.execute(self.insert_sql['fields'],
                #                  (self.field_id, self.record_id, field.tag, i1, i2)
                #                  )
                subfields = field.subfields
                for code, data in [(subfields[i],subfields[i+1]) for i in range(0,len(subfields),2)]:
                    self.subfield_id += 1
                    self.sfld_lst.append((self.subfield_id, self.field_id, code, data))
                    #self.curs.execute(self.insert_sql['subfields'],
                    #                  (self.subfield_id, self.field_id, code, data)
                    #                  )
            except Exception, ex:
                if isinstance(field, pymark.field.ControlField):
                    print "aaa: <%s> <%s>" % (field.tag, field.data)
                elif field.tag not in ('FMT','SYS'):
                    print "AAA:",unicode(field)
                    #print "IS:",self.insert_sql['fields']
                    #print "IS:",self.insert_sql['subfields']
                    #print "ID:",(self.subfield_id, self.field_id, code, data)
                print ex
        #print self.insert_sql['fields']
        #print lst
        
    def write(self):
        try:
            self.curs.execute("BEGIN")
            #print "records:",self.rec_lst
            self.curs.executemany(self.insert_sql['records'], self.rec_lst)
            #print "fields:"
            self.curs.executemany(self.insert_sql['fields'], self.fld_lst)
            #print "subfields:"
            self.curs.executemany(self.insert_sql['subfields'], self.sfld_lst)
        except Exception, ex:
            #print "write:",ex
            self.curs.execute("ROLLBACK")
        else:
            self.curs.execute("COMMIT")
        finally:
            self.rec_lst, self.fld_lst, self.sfld_lst = [],[],[]
            

    def load(self, fin):

        def fsize(f):
            cur = f.tell()
            f.seek(0, 2)
            s = f.tell()
            f.seek(cur)
            return s

        reader = pymarc.MARCReader(fin, to_unicode=True)
        size = fsize(fin)
        t0 = time()
        rc = clock()
        drc, dwc, elapsed, rest = 0, 0, 0, 0
        cur_pos = 0
        self.rec_lst, self.fld_lst, self.sfld_lst = [],[],[]
        for record in reader:
            rec_start_pos = cur_pos
            cur_pos = fin.tell()
            drc += clock() - rc
            rc = clock()
            try:
                wc = clock()
                self.append(record)
                dwc += clock() - wc
            except Exception,ex:
                print >> sys.stderr, cur_pos, timedelta(seconds=dwc)
                raise
            if self.record_id > self.options.total: break
            if self.record_id % self.options.batch_size == 0:
                self.write()
            if self.record_id % self.options.log_batch_size == 0:
                t1 = time()
                elapsed = t1 - t0
                rest = (size-cur_pos)*elapsed/cur_pos
                print >> sys.stderr, self.record_id, rec_start_pos, cur_pos-rec_start_pos,
                print >> sys.stderr, timedelta(seconds=drc), timedelta(seconds=dwc),
                print >> sys.stderr, timedelta(seconds=elapsed), timedelta(seconds=rest)
                drc, dwc = 0, 0
        self.write()

class SQLiteWriter(DBWriter):

    table_exists_sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?"

    def connect(self, options):
        import sqlite3
        dbfile = options.database
        if os.path.exists(dbfile) and options.overwrite:
            os.remove(dbfile)
        self.conn = sqlite3.connect(dbfile, isolation_level=None)
        self.curs = self.conn.cursor()
        self.curs.execute("PRAGMA cache_size=10000")
        self.curs.execute("PRAGMA synchronous=0")
        self.curs.execute("PRAGMA journal_mode=MEMORY")
        self.curs.execute("BEGIN")

    def close(self, commit=True):
        if commit: self.conn.commit()
        self.conn.close()



class PgWriter(DBWriter):

    paramstyle = '%s'

    table_exists_sql = "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=%s"

    def connect(self, options):
        try:
            import psycopg2
        except ImportError:
            error("psycopg2 модуль не найден. Скачать его можно здесь: \n"
                  "Linux - http://initd.org/projects/psycopg2\n"
                  "Windows - http://stickpeople.com/projects/python/win-psycopg")
        dsn = {}
        if options.host and options.host != 'localhost':
            dsn['host'] = options.host
        if options.port:
            dsn['port'] = options.port
        if options.user:
            dsn['user'] = options.user
        if options.password:
            dsn['password'] = options.password
        if options.database:
            dsn['dbname'] = options.database
        dsn = ' '.join(["%s=%s" % (k,v) for (k,v) in dsn.items()])
        self.conn = psycopg2.connect(dsn)
        self.conn.set_isolation_level(2)
        self.curs = self.conn.cursor()



class MySqlWriter(DBWriter):

    paramstyle = '%s'

    table_exists_sql = "SHOW TABLES LIKE %s"

    def connect(self, options):
        try:
            import MySQLdb
        except ImportError:
            error("MySQLdb модуль не найден. Скачать его можно здесь: \n"
                  "Linux - http://sourceforge.net/projects/mysql-python\n"
                  "Windows - http://www.codegood.com/archives/4")
        dsn = {}
        if options.host:
            dsn['host'] = options.host
        if options.port:
            dsn['port'] = options.port
        if options.user:
            dsn['user'] = options.user
        if options.password:
            dsn['passwd'] = options.password
        if options.database:
            dsn['db'] = options.database
        self.conn = MySQLdb.connect(**dsn)
        self.curs = self.conn.cursor()



def start_logging(filename):
    log.basicConfig(level=log.DEBUG,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M",
                    filename=filename,
                    filemode='w')
    console = log.StreamHandler()
    console.setLevel(log.INFO)
    formatter = log.Formatter("%(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    log.getLogger('').addHandler(console)
    

#kernprof.py -l ~/books/MARC/marcout/marc2db.py
#python -m line_profiler ~/books/MARC/marcout/marc2db.py.lprof

DBWRITERS = dict(sqlite = SQLiteWriter,
                 postgres = PgWriter,
                 mysql = MySqlWriter
                 )

if __name__ == '__main__':


    parser = OptionParser(usage=__doc__.decode('utf-8'))
    a = parser.add_option

    a("-t", "--database-type", dest="dbtype", default=None,
      help=u"Тип базы данных для записи. Допустимые значения: "
           u"%s. По умолчанию: %%default." % ', '.join(DBWRITERS.keys())
      )

    a("-D", "--database", dest="database", default=None,
      help=u"Имя БД для записи. В случае SQLite - имя файла БД.")
    a("-H", "--host", dest="host", default='localhost',
      help=u"IPадрес или имя хоста сервера БД. По умолчанию: %default.")
    a("-P", "--port", dest="port", default=None,
      help=u"Номер порта БД. По умолчанию: стандартный для БД порт.")
    a("-U", "--user", dest="user", default=None,
      help=u"Имя пользователя БД. По умолчанию: системное имя текущего пользователя.")
    a("-p", "--password", dest="password", default=None,
      help=u"Пароль пользователя БД.")

    a("-r", "--records-table", dest="records_table", default="marc_records",
      help=u"Имя таблицы заголовков MARC записей. По умолчанию: %default")
    a("-f", "--fields-table", dest="fields_table", default="marc_fields",
      help=u"Имя таблицы кодов полей. По умолчанию: %default")
    a("-s", "--subfields-table", dest="subfields_table", default="marc_subfields",
      help=u"Имя таблицы подполей. По умолчанию: %default")

    a("-S", "--start-pos", dest="start_pos", type='int', default=0,
      help=u"Начальная позиция во входном файле. По умолчанию: %default")
    a("-T", "--total", dest="total", type='int', default=sys.maxint,
      help=u"Максимальное число записей. По умолчанию: %default")
    a("-b", "--batch-size", dest="batch_size", type='int', default=1000,
      help=u"Число MARC записей в одном пакете при записи в БД. По умолчанию: %default")
    a("-B", "--log-batch-size", dest="log_batch_size", type='int', default=5000,
      help=u"Число MARC записей в одном пакете при записи в БД. По умолчанию: %default")
    a("-o", "--overwrite", dest="overwrite", action="store_true", default=False,
      help=u"Если выходные таблицы уже существуют, почистить их. По умолчанию - не чистить.")

    (options, args) = parser.parse_args()

    if not options.database:
        parser.error("Не задано имя выходной БД")

    if not args:
        parser.error("Не задан входной файл")

    if options.dbtype not in DBWRITERS:
        parser.error("Недопустимый тип БД %s" % options.dbtype)

    start_logging(args[0]+".log")
    log.debug(sys.argv)
    
    dbwriter = DBWRITERS[options.dbtype](options)

    try:
        for marc_file in args:
            if not os.path.exists(marc_file):
                print >> sys.stderr, "Файл '%s' не найден" % marc_file
                continue
            else:
                try:
                    if marc_file[-4:].lower() == '.bz2':
                        import bz2
                        fin = bz2.BZ2File(marc_file, 'r')
                    else:
                        fin = open(marc_file, 'rb')
                    if options.start_pos:
                        fin.seek(options.start_pos)
                except:
                    print >> sys.stderr, "Ошибка при открытии файла '%s'" % marc_file
                    continue
            dbwriter.load(fin)
    finally:
        dbwriter.close()

# python marc2db2.py -t sqlite -D rsl01.db -S 2630223379 -b 1 -B 1 -o ../rsl01.mrc
