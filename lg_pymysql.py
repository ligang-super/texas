#!/usr/bin/env python
# #coding=utf-8

import sys
import os
import time
import socket
import pymysql
from redis import BlockingConnectionPool
from pymysql.constants import CR
import six
import converters

SLOW_THR = 0.5
MAX_ALLOWED_PACKET = 51200

convert_type = six.binary_type


def validate_fields(fields):
    if fields is None:
        fields = '*'
    elif not isinstance(fields, six.string_types):
        try:
            fields = ','.join(x for x in fields)
        except Exception:
            fields = '*'
    else:
        fields = fields.strip()
        if not len(fields):
            fields = '*'
    return fields


def validate_conditions(conditions, enc='utf-8'):
    if isinstance(conditions, six.string_types):
        return six.ensure_str(conditions)
    elif conditions:
        return '(' + ') AND ('.join(conditions) + ')'
    else:
        return ''


def duplicate_conditions(conditions):
    if isinstance(conditions, dict):
        conditions = conditions.get('conditions', None)
    if not conditions:
        return []
    if isinstance(conditions, six.string_types):
        return [conditions]
    else:
        # noinspection PyTypeChecker
        return list(conditions)


def duplicate_condvalues(condvalues, ret_list=True):
    if isinstance(condvalues, dict):
        condvalues = condvalues.get('condvalues', None)
    if not condvalues:
        return [] if ret_list else ()
    else:
        return list(condvalues) if ret_list else tuple(condvalues)


def add_in_statement(conditions, condvalues, field, values):
    """
    by ATP
    由于MySQLdb.connections.literal方法的bug，当values仅有一个时
    会导致生成的sql语句多一个逗号，所以只能单独处理
    bug2
    这种写法根本就不能处理字符串型的in，太sb了
    """
    if not values:
        return
    if isinstance(values, str):
        values = values.split(',')
    elif not isinstance(values, list) and not isinstance(values, tuple):
        values = list(values)
    if len(values) == 1:
        conditions.append('%s=%%s' % field)
        condvalues.append(values[0])
    else:
        conditions.append('%s in (%s)' % (field, ','.join(['%s'] * len(values))))
        condvalues.extend(values)


def add_not_in_statement(conditions, condvalues, field, values):
    if not values:
        return
    if isinstance(values, str):
        values = values.split(',')
    elif not isinstance(values, list) and not isinstance(values, tuple):
        values = list(values)
    if len(values) == 1:
        conditions.append('%s!=%%s' % field)
        condvalues.append(values[0])
    else:
        conditions.append('%s not in (%s)' % (field, ','.join(['%s'] * len(values))))
        condvalues.extend(values)


def add_like_statement(conditions, condvalues, field, values, liketype=3):
    if not values:
        return
    if isinstance(values, str):
        values = values.split(',')
    if liketype == 1:
        values = ['%s%%' % x for x in values if x]
    elif liketype == 2:
        values = ['%%%s' % x for x in values if x]
    else:
        values = ['%%%s%%' % x for x in values if x]

    if len(values) == 1:
        conditions.append('%s like %%s' % field)
        condvalues.append(values[0])
    else:
        conditions.append('(' + ' or '.join(['%s like %%s' % field] * len(values)) + ')')
        condvalues.extend(values)


class MysqlConnection(object):
    description_format = "Connection<host=%(host)s,port=%(port)s,db=%(dbname)s>"

    def __init__(self, **kwargs):
        self.conn, self.cursor = None, None
        self.dic_cur = True
        self.enc = ''
        self.valid = False
        self.pid = os.getpid()
        self.addr = ''

    def connect(self, host, port, dbuser, dbpass, dbname, dic_cur, enc, check_dbname=True):
        """
        Connect to specified server/database
        """
        self.addr = '{}:{}'.format(host, port)
        if not self.valid:
            t1 = time.time()

            self.conn = pymysql.connect(host=host, port=port, user=dbuser, passwd=dbpass, use_unicode=six.PY3,
                                        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor,
                                        autocommit=True)
            if sys.platform != 'darwin':
                # noinspection PyProtectedMember
                self.conn._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            # noinspection PyProtectedMember
            self.conn._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            # noinspection PyProtectedMember
            self.conn._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 2)
            self.cursor = self.conn.cursor()
            self.dic_cur = dic_cur
            self.conn.set_charset('utf8mb4')
            self.cursor.execute("SET NAMES " + enc)
            self.cursor.execute("SET CHARACTER_SET_CLIENT=" + enc)
            self.cursor.execute("SET CHARACTER_SET_RESULTS=" + enc)
            if dbname:
                if check_dbname:
                    self.cursor.execute("SHOW DATABASES")
                    r = self.cursor.fetchall()
                    if dbname in [i['Database'] for i in r]:
                        self.cursor.execute("USE %s" % dbname)
                else:
                    self.cursor.execute("USE %s" % dbname)
            self.enc = enc
            self.valid = True
            t2 = time.time()


    def disconnect(self):
        self.valid = False
        if self.cursor:
            try:
                self.cursor.close()
                self.cursor = None
            except pymysql.Error as e:
                pass
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
            except pymysql.Error as e:
                if e.message == "Already closed":
                    pass


    @staticmethod
    def literal(one_arg):
        _t = type(one_arg)
        if _t in six.integer_types:
            return one_arg
        conv = converters.encoders.get(_t)
        if conv:
            return conv(one_arg, None)
        else:
            return converters.encoders[str](str(one_arg), None)

    def format_args(sqlargs):
        # python2要将unicode转成string, python3要将bytes转成string
        return tuple(MysqlConnection.literal(six.ensure_str(o) if isinstance(o, convert_type) else o) for o in sqlargs)

    def query(self, sqlstr, sqlargs=(), **kwargs):
        t1 = time.time()
        sqlstr = six.ensure_str(sqlstr)
        if sqlargs:
            cmd = sqlstr % MysqlConnection.format_args(sqlargs)
        else:
            cmd = sqlstr

        max_allowed_packet = MAX_ALLOWED_PACKET if not kwargs.get('max_allowed_packet') \
            else kwargs['max_allowed_packet']
        if len(cmd) >= max_allowed_packet:
            print('sql=%s is toooo long(max=%s) %s' % (len(cmd), max_allowed_packet, cmd[:300]))
        self.cursor.execute(cmd)
        rs = self.cursor.fetchall()
        rows = []
        for row in rs:
            conv_row = {str(i): j for i, j in six.iteritems(row)}
            rows.append(conv_row)

        t2 = time.time()

        return rows

    def execute(self, sqlstr, sqlargs=(), logger=None, **kwargs):
        t1 = time.time()
        sqlstr = six.ensure_str(sqlstr)
        if sqlargs:
            cmd = sqlstr % MysqlConnection.format_args(sqlargs)
        else:
            cmd = sqlstr
        max_allowed_packet = MAX_ALLOWED_PACKET if not kwargs.get('max_allowed_packet') \
            else kwargs['max_allowed_packet']
        if len(cmd) >= max_allowed_packet:
            print('EXCEPTION: sql=%s is toooo long(max=%s) %s' % (len(cmd), max_allowed_packet, cmd[:300]))
        self.cursor.execute(cmd)

        # 此处的case，数据被截断的时候表现为插入成功，返回的insert_id为0
        # cursor会始终保存本次正确执行的结果，conn上会保存sql执行的结果，如果发生警告等信息，conn会重新去拉下警告信息
        # 此时会导致conn上次保存的结果被警告信息覆盖，所以此时去conn上取insert_id是空的，正确的应该用cursor
        insert_id = self.cursor.lastrowid
        rowcount = self.cursor.rowcount
        t2 = time.time()

        return rowcount, insert_id

    def binary_execute(self, sqlstr, sqlargs=(), **kwargs):
        t1 = time.time()
        self.cursor.execute(six.ensure_str(sqlstr), sqlargs)

        # 此处的case，数据被截断的时候表现为插入成功，返回的insert_id为0
        # cursor会始终保存本次正确执行的结果，conn上会保存sql执行的结果，如果发生警告等信息，conn会重新去拉下警告信息
        # 此时会导致conn上次保存的结果被警告信息覆盖，所以此时去conn上取insert_id是空的，正确的应该用cursor
        insert_id = self.cursor.lastrowid
        rowcount = self.cursor.rowcount
        t2 = time.time()

        return rowcount, insert_id


class ConnectionPool(BlockingConnectionPool):
    def __init__(self, cls_name, pool_size,
                 host='localhost', port=3306, dbname='', readonly=True,
                 dbuser='', dbpass='', dic_cur=True, enc='utf8mb4', logger=None, timeout=5,
                 check_dbname=True, **kwargs):
        self._cls = cls_name
        self.pool_size = pool_size
        self.host = host
        self.port = port
        self.dbuser, self.dbpass = dbuser, dbpass
        self.dbname = dbname
        self.check_dbname = check_dbname
        self.dic_cur = dic_cur
        self.enc = enc
        super(ConnectionPool, self).__init__(connection_class=cls_name, max_connections=pool_size, timeout=timeout,
                                             host=host, port=port, dbname=dbname)

    def release(self, connection):
        """Releases the connection back to the pool."""
        # Make sure we haven't changed process.
        self._checkpid()
        if connection and connection.pid != self.pid:
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def force_close_connection(self, connection):
        try:
            connection.disconnect()
        except:
            pass

    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        try:
            # ensure this connection is connected to Redis
            #connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            pass
            #try:
            #    if connection.can_read():
            #        raise ConnectionError("Connection has data")
            #except ConnectionError:
            #    connection.disconnect()
            #    connection.connect()
            #    if connection.can_read():
            #        raise ConnectionError("Connection not ready")
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            self.release(connection)
            raise

        return connection

    def _safe_run(self, func, action='', defval=None, verbose=False, **kwargs):
        # 如果获取不到连接(连接池都用光了)，则直接在此行exception(redis.ConnectionError)
        # 如果可以获取成功，则一定是个MysqlConnection instance,但不一定是正在连接的状态(可能没有初始化连接失败，也可能连接断了)
        t1 = time.time()
        connection = self.get_connection(command_name='mysql')
        t2 = time.time()
        if t2 - t1 > 0.1:
            self.logger.warn('pymysql get_connection slow, %s' % (t2 - t1))
        try:
            connection.connect(host=self.host, port=self.port, dbuser=self.dbuser,
                               dbpass=self.dbpass, dbname=self.dbname, dic_cur=self.dic_cur,
                               enc=self.enc, check_dbname=self.check_dbname)
            t3 = time.time()
            if t3 - t2 > 0.2:
                self.logger.warn('pymysql connection_connect slow, %s' % (t3 - t2))
            t1 = time.time()
            r = func(connection, action=action, verbose=verbose, **kwargs)
            # r = gevent.with_timeout(5, func, connection, action=action, verbose=verbose, **kwargs)
            self.log_slow(action=action, dur=time.time() - t1, verbose=verbose, **kwargs)
            return r
        except (pymysql.ProgrammingError, pymysql.InternalError, pymysql.IntegrityError) as e:
            # 缺表、缺行、执行错误
            print('ERROR: taou_pymysql execute_command1(%s) err=%s, sqlstr=%s, sqlargs=%s' % (
                self.host, e, kwargs.get('sqlstr'), kwargs.get('sqlargs')))
        except Exception as e:
            error_log = 'taou_pymysql execute_command2(%s) err=%s(%s), sqlstr=%s, sqlargs=%s' % (
                self.host, e, type(e), kwargs.get('sqlstr'), kwargs.get('sqlargs'))
            if isinstance(e, pymysql.OperationalError) and e.args[0] in (CR.CR_SERVER_GONE_ERROR, CR.CR_SERVER_LOST):
                print(error_log)
            else:
                print(error_log)
            # force reset connection as initial state to prepare re-connect again.
            self.force_close_connection(connection)

            raise
        finally:
            self.release(connection)


    def safe_run(self, **kwargs):
        pool_size = self.pool_size + 1
        while pool_size >= 0:
            try:
                print(kwargs)
                return self._safe_run(**kwargs)
            except pymysql.OperationalError as e:
                if e.args[0] in (CR.CR_SERVER_GONE_ERROR, CR.CR_SERVER_LOST):
                    # 如果一个数据库连接很久没有使用，则可能会被断开，这种情况下，需要重新执行该数据库请求, 最多执行pool_size次.
                    pool_size -= 1
                    self.logger.warn('safe_run SERVER GONE OR LOST(%s/%s)' % (pool_size, self.pool_size))
                    continue
                else:
                    self.logger.error('safe_run exception=%s (%s/%s)' % (e, pool_size, self.pool_size))
                    raise
        raise Exception(
            'safe_run loop (%s/%s) by connection lost but still failed' % (pool_size, self.pool_size + 1))

    def log_slow(self, action, dur, verbose=False, **kwargs):
        if dur > SLOW_THR:
            sstr = '[SLOW]'
        elif verbose:
            sstr = ''
        else:
            return
        func = self.logger.error if dur > 5 else self.logger.debug
        if 'sqlstr' in kwargs:
            sqlstr = kwargs['sqlstr']
            if len(sqlstr) > 1024:
                sqlstr = sqlstr[:1024] + '...'
            if 'sqlargs' in kwargs:
                sqlargs = str(kwargs['sqlargs'])
                if len(sqlargs) > 32:
                    sqlargs = sqlargs[:30] + '...'
                func('%s%s[%s] sql=%s,args=%s,time=%s', sstr, action, self.host, sqlstr, sqlargs, dur)
            else:
                func('%s%s[%s] sql=%s,time=%s', sstr, action, self.host, sqlstr, dur)
        else:
            func('%s%s[%s] time=%s', sstr, action, self.host, dur)

    def general_query(self, sqlstr, sqlargs=(), defval=None, action='query', verbose=False, **kwargs):
        return self.safe_run(func=self._cls.query, action=action, defval=defval, sqlstr=sqlstr, sqlargs=sqlargs,
                             verbose=verbose, **kwargs)

    def general_execute(self, sqlstr, sqlargs=(), defval=None, action='execute', verbose=False, **kwargs):
        return self.safe_run(func=self._cls.execute, action=action, defval=defval, sqlstr=sqlstr, sqlargs=sqlargs,
                             verbose=verbose, **kwargs)

    def binary_execute(self, sqlstr, sqlargs=(), defval=None, action='binary_execute', verbose=False, **kwargs):
        return self.safe_run(func=self._cls.binary_execute, action=action, defval=defval, sqlstr=sqlstr, sqlargs=sqlargs,
                             verbose=verbose, logger=self.logger, **kwargs)



class MysqlClientPool(ConnectionPool):
    def __init__(self, pool_size=1, cls_name=None, host='localhost', port=3306, dbname='', readonly=True,
                 dbuser='', dbpass='', dic_cur=True, enc='utf8mb4', logger=None, check_dbname=True, **kwargs):

        ConnectionPool.__init__(self, pool_size=pool_size, cls_name=cls_name, host=host, port=port,
                                dbname=dbname, readonly=readonly, dbuser=dbuser, dbpass=dbpass, dic_cur=dic_cur,
                                enc=enc, logger=logger, check_dbname=check_dbname, **kwargs)
        self.tbfields = {}

    def get_data(self, table, conditions=(), condvalues=(), fields=None, orderby=None, limit=None, force_index=None,
                 join_table=None, groupby=None, having=None, verbose=False, action='GET', **kwargs):
        """
        cond_values is for back compatibles
        prefer use condvalues
        """

        sqlstr, sqlargs = self.build_select_sql(table, conditions, condvalues, fields, orderby, limit, force_index,
                                                join_table, groupby, having, verbose, action, **kwargs)

        return self.general_query(sqlstr=sqlstr, sqlargs=sqlargs, action=action, verbose=verbose)

    def get_data_by_kvs(self, table, conditions=(), condvalues=(), fields=None, orderby=None, limit=None,
                        force_index=None, groupby=None, action='GET/KVS', verbose=False, **kwargs):
        cond = duplicate_conditions(conditions)
        condv = duplicate_condvalues(condvalues)
        if kwargs:
            for k, v in six.iteritems(kwargs):
                if v is None:
                    cond.append('%s is NULL' % k)
                else:
                    cond.append('%s=%%s' % k)
                    condv.append(v)
        return self.get_data(table=table, fields=fields, orderby=orderby, limit=limit, conditions=cond,
                             condvalues=condv, force_index=force_index, groupby=groupby, action=action, verbose=verbose)

    def get_single_data(self, table, action='GET_SINGLE', **kwargs):
        kwargs['limit'] = 1
        x = self.get_data(table=table, action=action, **kwargs)
        if x:
            return x[0]
        else:
            return {} if self.dic_cur else []

    def get_single_data_by_kvs(self, table, action='GET_SINGLE_KVS', **kwargs):
        kwargs['limit'] = 1
        x = self.get_data_by_kvs(table=table, action=action, **kwargs)
        if x:
            return x[0]
        else:
            return {} if self.dic_cur else []

    def get_data_by_id(self, table, idx, id_field='id', action='GET_IDX', **kwargs):
        """
        根据某个field的值取单个数据行
        """
        conditions = duplicate_conditions(kwargs.pop('conditions', None))
        condvalues = duplicate_condvalues(kwargs.pop('condvalues', None))
        conditions.append('`%s`=%%s' % id_field)
        condvalues.append(idx)
        return self.get_single_data(table=table, conditions=conditions, condvalues=condvalues, action=action, **kwargs)

    def get_datas_by_id(self, table, idx, id_field='id', action='GETS_IDX', **kwargs):
        """
        根据某个id取多个数据行
        """
        conditions = duplicate_conditions(kwargs.pop('conditions', None))
        condvalues = duplicate_condvalues(kwargs.pop('condvalues', None))
        conditions.append('`%s`=%%s' % id_field)
        condvalues.append(idx)
        return self.get_data(table=table, conditions=conditions, condvalues=condvalues, action=action, **kwargs)

    def get_data_in_ids(self, table, ids, id_field='id', action='GET_IDS', **kwargs):
        """
        根据给定多个id取相应的数据行
        """
        if not ids:
            return []
        conditions = duplicate_conditions(kwargs.pop('conditions', None))
        condvalues = duplicate_condvalues(kwargs.pop('condvalues', None))
        add_in_statement(conditions, condvalues, id_field, ids)
        return self.get_data(table=table, conditions=conditions, condvalues=condvalues, action=action, **kwargs)

    get_datas_in_ids = get_data_in_ids

    def get_data_in_ids_as_dict(self, table, ids, id_field='id', action='GET_DATA_IDS_DCT', **kwargs):
        """
        根据多个id去数据行，每个id只取一行
        """
        items = self.get_data_in_ids(ids=ids, table=table, id_field=id_field, action=action, **kwargs)
        ret = {}
        for item in items:
            ret[item[id_field]] = item
        return ret

    def get_datas_in_ids_as_dict(self, table, ids, id_field='id', action='GET_DATAS_IDS_DCT', **kwargs):
        """
        根据多个id去数据行，每个id取多行
        """
        items = self.get_data_in_ids(table=table, ids=ids, id_field=id_field, action=action, **kwargs)
        ret = {}
        for item in items:
            lst = ret.setdefault(item[id_field], [])
            lst.append(item)
        return ret

    def insert_data_binary(self, table, val_dict, has_crtime=False, crtime_field='crtime', action='insert_data_binary',
                           insert_ignore=False, verbose=False, **kwargs):
        """插入内容包含二进制"""
        fields = []
        values = []

        for key, value in six.iteritems(val_dict):
            fields.append(key)
            values.append(value)

        if insert_ignore:
            p1 = 'INSERT IGNORE INTO '
        else:
            p1 = 'INSERT INTO '

        # execute
        if not has_crtime or crtime_field in val_dict:
            sqlstr = p1 + '%s(`%s`) VALUES(%s)' % (table, '`,`'.join(fields), ','.join(['%s'] * len(val_dict)))
        else:
            sqlstr = p1 + '''%s(`%s`, %s)
             VALUES(%s, CURRENT_TIMESTAMP)''' % (
                table, '`,`'.join(fields), crtime_field, ','.join(['%s'] * len(val_dict)))
        values = tuple(values)

        r = self.binary_execute(sqlstr=sqlstr, sqlargs=values)

        if not r:
            return -1
        try:
            _, insert_id = r
            return insert_id
        except:
            return -1

    def insert_data(self, table, val_dict, has_crtime=False, crtime_field='crtime', action='insert_data',
                    insert_ignore=False, verbose=False, **kwargs):
        """

        :param table:
        :param val_dict:
        :param has_crtime:
        :param crtime_field:
        :param action:
        :param insert_ignore:
        :param verbose:
        :param kwargs:
        :return: -1|0: 插入失败; >0: 成功, 插入之后的row id
        """

        fields = []
        values = []
        for key, value in six.iteritems(val_dict):
            fields.append(key)
            values.append(value)

        if insert_ignore:
            p1 = 'INSERT IGNORE INTO '
        else:
            p1 = 'INSERT INTO '

        # execute
        if not has_crtime or crtime_field in val_dict:
            sqlstr = p1 + '%s(`%s`) VALUES(%s)' % (table, '`,`'.join(fields), ','.join(['%s'] * len(val_dict)))
        else:
            sqlstr = p1 + '''%s(`%s`, %s)
             VALUES(%s, CURRENT_TIMESTAMP)''' % (
                table, '`,`'.join(fields), crtime_field, ','.join(['%s'] * len(val_dict)))
        values = tuple(values)
        r = self.general_execute(sqlstr=sqlstr, sqlargs=values, action=action, verbose=verbose,
                                 max_allowed_packet=kwargs.get('max_allowed_packet'))
        if not r:
            return -1
        try:
            rowcount, insert_id = r
            return insert_id
        except:
            return -1

    insert_quiet = insert_data

    def insert_data_list(self, table, fields, values, has_crtime=False, crtime_field='crtime', action='INSERT',
                         verbose=False, **kwargs):
        """

        :param fields: 一维数组，列名的list/tuple
        :param values: 二维数组，多个value list/tuple的list/tuple
        :param table:
        :param has_crtime:
        :param crtime_field:
        :param kwargs:
        :return:
        """
        ll = len(values)
        if not ll:
            return 0

        if not has_crtime:
            sqlstr = 'INSERT INTO %s (`%s`) VALUES ' % (table, '`,`'.join(fields))
            sfmt = '(' + ','.join(['%s'] * len(fields)) + ')'
        else:
            sqlstr = 'INSERT INTO %s (`%s`, %s) VALUES ' % (table, '`,`'.join(fields), crtime_field)
            sfmt = '(' + ','.join(['%s'] * len(fields)) + ',CURRENT_TIMESTAMP)'
        sfmts = ','.join([sfmt] * len(values))
        sqlstr += sfmts
        all_values = []
        for vv in values:
            all_values.extend(vv)

        r = self.general_execute(sqlstr=sqlstr, sqlargs=all_values, action=action, verbose=verbose)
        if not r:
            return -1
        try:
            rowcount, insert_id = r
            return 1
        except:
            return -1

    def insert_or_update_data(self, table, val_dict=None, increases=None, decreases=None, bitors=None, has_crtime=False,
                              crtime_field='crtime', action='INSERT_UPDATE', verbose=False, isup_infos=None, **kwargs):
        """
        确实插入数据或者数据有变化时，返回primary key
        否则返回0

        isup_infos.insert_id 为插入或者变化的那一行的primary key
        isup_infos.rowcount  为命中的rowcount
        分三种情况：
        1. 真正插入数据时:
            insert_id>0 and rowcount=1
        2. 修改已有数据时:
            insert_id>0 and rowcount>1 (typically=2)
        3. 没有插入也没有修改:
            insert_id=0 and rowcount=1

        """
        # prepare
        fields, values = [], []
        upfields = []
        if val_dict:
            for key, value in six.iteritems(val_dict):
                fields.append(key)
                values.append(value)
                if value is not None:
                    upfields.append(
                        '`%s`=VALUES(`%s`)' % (key, key))
        if increases:
            for key, value in six.iteritems(increases):
                fields.append(key)
                values.append(value)
                upfields.append(
                    '`%s`=`%s`+VALUES(`%s`)' % (key, key, key))
        if decreases:
            for key, value in six.iteritems(decreases):
                fields.append(key)
                values.append(-value)
                upfields.append(
                    '`%s`=`%s`+VALUES(`%s`)' % (key, key, key))
        if bitors:
            for key, value in six.iteritems(bitors):
                fields.append(key)
                values.append(value)
                upfields.append('`%s`=`%s`|VALUES(`%s`)' % (key, key, key))

        # execute
        if not has_crtime or crtime_field in set(fields):
            sqlstr = '''INSERT INTO %s(`%s`) VALUES(%s)''' % (table, '`,`'.join(fields), ','.join(['%s'] * len(fields)))
        else:
            sqlstr = '''INSERT INTO %s(`%s`, %s)
             VALUES(%s, CURRENT_TIMESTAMP)''' % (
                table, '`,`'.join(fields), crtime_field, ','.join(['%s'] * len(fields)))
        if len(upfields):
            sqlstr += ' ON DUPLICATE KEY UPDATE '
            sqlstr += ','.join(upfields)

        values = tuple(values)

        r = self.general_execute(sqlstr=sqlstr, sqlargs=values, action=action, verbose=verbose)
        if not r:
            return -1
        try:
            rowcount, insert_id = r
            if isinstance(isup_infos, dict):
                isup_infos.update({'rowcount': rowcount, 'insert_id': insert_id})
            return insert_id
        except:
            return -1

    def insert_or_update_data_list(self, fields, values, table, has_crtime=False, crtime_field='crtime',
                                   action='INSERT_UPDATES', verbose=False, **kwargs):
        ll = len(values)
        if not ll:
            return 0

        if not has_crtime:
            sqlstr = 'INSERT INTO %s (`%s`) VALUES ' % (table, '`,`'.join(fields))
            sfmt = '(' + ','.join(['%s'] * len(fields)) + ')'
        else:
            sqlstr = 'INSERT INTO %s (`%s`, %s) VALUES ' % (table, '`,`'.join(fields), crtime_field)
            sfmt = '(' + ','.join(['%s'] * len(fields)) + ',CURRENT_TIMESTAMP)'
        sfmts = ','.join([sfmt] * len(values))
        sqlstr += sfmts
        sqlstr += ' ON DUPLICATE KEY UPDATE '
        sqlstr += ','.join(['`%s`=VALUES(`%s`)' % (x, x) for x in fields])
        all_values = []
        for vv in values:
            all_values.extend(vv)

        r = self.general_execute(sqlstr=sqlstr, sqlargs=all_values, action=action, verbose=verbose)
        if not r:
            return -1
        try:
            rowcount, insert_id = r
            return 1
        except:
            return -1

    def update_data(self, table, val_dict=None, conditions=None, condvalues=(), force_index=None, limit=None,
                    increases=None, decreases=None, bitors=None, keep_uptime=False, action='UPDATE', verbose=False,
                    bitands=None, **kwargs):
        """

        :param table:
        :param val_dict:
        :param conditions:
        :param condvalues:
        :param force_index:
        :param limit:
        :param increases:
        :param decreases:
        :param bitors:
        :param keep_uptime:
        :param action:
        :param verbose:
        :param bitands:
        :param kwargs:
        :return: -1: 失败; >=0 affected_rows, 如果该行内容没有修改(即新设置的值跟原来一样)则返回0.
        """
        if conditions is None:
            raise Exception('conditions cannot be None! You must be wrong!!')
        fields = []
        values = []
        if val_dict:
            for key, value in six.iteritems(val_dict):
                fields.append('`%s`=%%s' % key)
                values.append(value)
        if increases:
            for key, value in six.iteritems(increases):
                fields.append('`%s`=`%s`+%%s' % (key, key))
                values.append(value)
        if decreases:
            for key, value in six.iteritems(decreases):
                fields.append('`%s`=`%s`-%%s' % (key, key))
                values.append(value)
        if bitors:
            for key, value in six.iteritems(bitors):
                fields.append('`%s`=`%s`|%%s' % (key, key))
                values.append(value)
        if bitands:
            for key, value in six.iteritems(bitands):
                fields.append('`%s`=`%s`&%%s' % (key, key))
                values.append(value)
        if keep_uptime:
            fields.append('`uptime`=`uptime`')

        # execute
        sqlstr = 'UPDATE %s ' % table
        if force_index:
            sqlstr += 'FORCE INDEX(`%s`) ' % force_index
        sqlstr += 'SET %s' % (','.join(fields))

        if conditions:
            sqlstr += ' WHERE '
            sqlstr += validate_conditions(conditions)
        if limit is not None:
            sqlstr += ' LIMIT '
            sqlstr += str(limit)

        sqlargs = tuple(values) + tuple(condvalues)
        r = self.general_execute(sqlstr=sqlstr, sqlargs=sqlargs, action=action, verbose=verbose)
        if not r:
            return -1
        try:
            rowcount, insert_id = r
            return rowcount
        except:
            return -1

    def update_data_by_id(self, table, idx, id_field='id', conditions=None, condvalues=(), action='UPDATE_IDX',
                          verbose=False, **kwargs):
        conditions = duplicate_conditions(conditions)
        condvalues = duplicate_condvalues(condvalues)
        conditions.append('`%s`=%%s' % id_field)
        condvalues.append(idx)
        return self.update_data(table=table, conditions=conditions, condvalues=condvalues, action=action,
                                verbose=verbose, **kwargs)

    def update_data_in_ids(self, table, ids, id_field='id', conditions=None, condvalues=(), action='UPDATE_IDS',
                           verbose=False, **kwargs):
        if not ids:
            return
        conditions = duplicate_conditions(conditions)
        condvalues = duplicate_condvalues(condvalues)
        add_in_statement(conditions, condvalues, id_field, ids)
        return self.update_data(table=table, conditions=conditions, condvalues=condvalues, action=action,
                                verbose=verbose, **kwargs)

    def delete_data(self, table, conditions=None, condvalues=(), action='DELETE', verbose=False, **kwargs):
        sqlstr = 'DELETE FROM %s' % table
        if conditions:
            sqlstr += ' WHERE '
            sqlstr += validate_conditions(conditions)
        else:
            self.logger.error('delete_data err=no_cond')
            return False

        sqlargs = tuple(condvalues)
        r = self.general_execute(sqlstr=sqlstr, sqlargs=sqlargs, action=action, verbose=verbose)
        if not r:
            return False
        try:
            rowcount, insert_id = r
            return True
        except:
            return False

    def delete_data_by_id(self, table, idx, id_field='id', conditions=None, condvalues=(), action='DELETE_IDX',
                          verbose=False, **kwargs):
        conditions = duplicate_conditions(conditions)
        condvalues = duplicate_condvalues(condvalues)
        conditions.append('`%s`=%%s' % id_field)
        condvalues.append(idx)
        return self.delete_data(table=table, conditions=conditions, condvalues=condvalues, action=action,
                                verbose=verbose, **kwargs)

    def delete_data_in_ids(self, table, ids, id_field='id', conditions=None, condvalues=(), action='DELETE_IDS',
                           verbose=False, **kwargs):
        if not ids:
            return
        conditions = duplicate_conditions(conditions)
        condvalues = duplicate_condvalues(condvalues)
        add_in_statement(conditions, condvalues, id_field, ids)
        return self.delete_data(table=table, conditions=conditions, condvalues=condvalues, action=action,
                                verbose=verbose, **kwargs)

    def get_maximum_line(self, table, field='id', **kwargs):
        sqlstr = 'SELECT * FROM %s ORDER BY %s DESC LIMIT 1' % (table, field)
        r = self.general_query(sqlstr=sqlstr)
        return r[0] if r else {} if self.dic_cur else tuple()

    def get_maximum_id(self, table, primary_field='id', **kwargs):
        r = self.get_maximum_line(table=table, field=primary_field)
        return None if not r else r[primary_field] if self.dic_cur else r[0]

    def show_fields(self, table, **kwargs):
        sqlstr = 'SHOW FIELDS FROM %s' % table
        return self.general_query(sqlstr=sqlstr, **kwargs)

    def get_table_fields(self, table, **kwargs):
        rr = self.show_fields(table=table)
        fr = []
        for one in rr:
            if self.dic_cur:
                fdname = one['Field']
            else:
                fdname = one[0]
            fr.append(fdname)
        return fr

    def filter_valid_fields(self, val_dict, table):
        """
        Remove un-existence key for val_dict according to table
        param       default     descrition
        val_dict                the dict of values to be filtered
        table                   table name
        """
        if not val_dict:
            return {}
        val_dict2 = {}
        fields = self.tbfields.setdefault(table, set(self.get_table_fields(table=table)))
        for k, v in six.iteritems(val_dict):
            if k in fields:
                val_dict2[k] = str(v)
        return val_dict2

    def build_select_sql(self, table, conditions=(), condvalues=(), fields=None, orderby=None, limit=None, force_index=None,
                         join_table=None, groupby=None, having=None, verbose=False, action='GET', **kwargs):

        trace_attrs = {'db.table': table}
        fields = validate_fields(fields)
        sqlstr = 'SELECT %s FROM %s' % (fields, table)
        if force_index:
            sqlstr += ' FORCE INDEX(`%s`) ' % force_index
            trace_attrs['db.force_index'] = force_index
        if join_table:
            sqlstr += ' %s ' % join_table
            trace_attrs['db.join_table'] = join_table
        if conditions:
            sqlstr += ' WHERE '
            sqlstr += validate_conditions(conditions)
        if groupby is not None:
            sqlstr += ' GROUP BY '
            sqlstr += groupby
            trace_attrs['db.group_by'] = groupby
        if having is not None:
            sqlstr += ' HAVING '
            sqlstr += having
            trace_attrs['db.having'] = having
        if orderby is not None:
            sqlstr += ' ORDER BY '
            sqlstr += six.ensure_str(orderby)
            trace_attrs['db.order_by'] = orderby
        if limit is not None:
            sqlstr += ' LIMIT '
            sqlstr += str(limit)
            trace_attrs['db.limit'] = limit
        if condvalues:
            sqlargs = tuple(condvalues)
        else:
            sqlargs = ()

        trace_attrs['db.args.num'] = len(sqlargs)
        trace_attrs['db.query'] = sqlstr[:150]

        return sqlstr, sqlargs

    def get_min_id_after_crtime(self, table='', crtime='1970-01-01 00:00:00', equal=True, time_field='crtime'):
        """
        获取crtime >(=) x时候最小的id值
        查询这个id的目的，是将crtime >(=) x 转化成 id >(=) y
        这样能更好的利用二级索引过滤掉大部分数据，大规模避免回表
        同时满足下列前提后再使用,优化效果明显:
            1.table中id为自增主键,且不会被修改
            2.crtime仅在插入时写为当时时间戳，过后永不修改
            3.table有crtime的索引
            4.查询的时候用到了crtime圈范围
            5.SQL命中的索引中无法有效命中crtime列(或者索引中不含crtime列)
        :param table: 表名
        :param crtime: crtime的边界值
        :param equal: 是否含等号
        :param time_field: 指定哪一列是crtime
        :return: 最小id值，有就是有，0就是0，否则就是None
        """
        #
        op = '>'
        if equal:
            op = '>='
        try:
            item = self.get_single_data(table=table,
                                        conditions=['{} {} %s'.format(time_field, op)],
                                        condvalues=[crtime],
                                        orderby='{} asc'.format(time_field),
                                        fields=time_field)
            if not item:
                return None
            min_crtime = item[time_field]  # 不做in判断，有try兜底
            item = self.get_single_data(table=table,
                                        conditions=['{} = %s'.format(time_field)],
                                        condvalues=[min_crtime],
                                        fields='min(id) as min_id')
            return item['min_id']
        except Exception as _:
            return None

    def get_max_id_before_crtime(self, table='', crtime='1970-01-01 00:00:00', equal=True, time_field='crtime'):
        """
        获取crtime <(=) x时候最大的id值
        查询这个id的目的，是将crtime <(=) x 转化成 id <(=) y
        这样能更好的利用二级索引过滤掉大部分数据，大规模避免回表
        满足的条件见下函数，即可高效优化
        >>>get_min_id_after_crtime
        :param table: 表名
        :param crtime: crtime的边界值
        :param equal: 是否含等号
        :param time_field: 指定哪一列是crtime
        :return: 最小id值，有就是有，0就是0，否则就是None
        """
        #
        op = '<'
        if equal:
            op = '<='
        try:
            item = self.get_single_data(table=table,
                                        conditions=['{} {} %s'.format(time_field, op)],
                                        condvalues=[crtime],
                                        orderby='{} desc'.format(time_field),
                                        fields=time_field)
            if not item:
                return None
            max_crtime = item[time_field]  # 不做in判断，有try兜底
            item = self.get_single_data(table=table,
                                        conditions=['{} = %s'.format(time_field)],
                                        condvalues=[max_crtime],
                                        fields='max(id) as max_id')
            return item['max_id']
        except Exception as _:
            return None
