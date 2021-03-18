import psycopg2
from psycopg2 import extras  # 不能少
class PostGreSQL:
    # 初始化
    def __init__(self, dataBaseName, userName, password, host, port):
        self.dataBaseName = dataBaseName
        self.userName = userName
        self.password = password
        self.host = host
        self.port = port
        self._conn = self.GetConnect()
        if self._conn:
            self._cur = self._conn.cursor()

    # 获取数据库连接对象
    def GetConnect(self):
        conn = False
        try:
            conn = psycopg2.connect(
                database=self.dataBaseName,
                user=self.userName,
                password=self.password,
                host=self.host,
                port=self.port
                #,cursorclass = psycopg2.c
            )
        except Exception as err:
            print("连接数据库失败，%s" % err)
        return conn
    # 执行查询sql
    def ExecQuery(self, sql):
        res = ""
        try:
            self._cur.execute(sql)
            res = self._cur.fetchall()
            #cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)  # 字典形式返回游标
            self._cur.close()
        except Exception as err:
            print("查询失败, %s" % err)
        else:
            return res
    #批量插入数据
    def exceinto(self,sql, datalist):
        flag = False
        try:
            #extras.execute_values(cur, sql, datalist, page_size=20000)
            extras.execute_batch(self._cur,sql,datalist,page_size = len(datalist))
            self._conn.commit()
            flag = True
        except Exception as err:
            flag = False
            self._conn.rollback()
            print("执行失败, %s" % err)
        else:
            return flag
    # 执行增删改sql
    def ExceNonQuery(self, sql):
        flag = False
        try:
            self._cur.execute(sql)
            self._conn.commit()
            flag = True
        except Exception as err:
            flag = False
            self._conn.rollback()
            print("执行失败, %s" % err)
        else:
            return flag
    def close(self):

        self._conn.close()
    def GetConnectInfo(self):
        print("连接信息：")
        print("服务器:%s , 用户名:%s , 数据库:%s " % (self.host, self.userName, self.dataBaseName))
