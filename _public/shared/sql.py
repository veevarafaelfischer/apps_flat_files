from typing import Any

import pyodbc as pyodbc

class SQL:
    def __init__(self):
        pass

    def getConn(self) -> Any:
        server = '172.31.1.220'
        username = 'app_flatfile'
        password = 'FlaTf1file5@2023'
        conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=' + server + ';UID=' + username + ';PWD=' + password)
        return conn

    def __getcursor__(self):
        conn = self.getConn()
        return conn.cursor()

    def item(self, sql):
        with self.__getcursor__() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()

    def itemAll(self, sql):
        with self.__getcursor__() as cursor:
            cursor.execute(sql)
            return {
                "data": cursor.fetchall(),
                "columns": [desc[0] for desc in cursor.description]
            }

    def itemsList(self, sql):
        try:
            itens = []
            with self.__getcursor__() as cursor:
                #print(sql)
                cursor.execute(sql)
                records = cursor.fetchall()
                columnNames = [column[0] for column in cursor.description]

            for record in records:
                itens.append(dict(zip(columnNames, record)))

            return True, itens, len(itens)

        except Exception as ex:
            print('Erro ao executar SQL:', '\n-----------\n', sql, '\n-----------\n', ex, '\n-----------\n')
            return False, [], -1, ex


    def itemExecute(self, sql):
        try:
            with self.__getcursor__() as cursor:
                cursor.connection.autocommit = True
                # print(sql)
                cursor.execute(sql)
                total_retorno = cursor.rowcount
            return True, total_retorno, ''
        except Exception as ex:
            print('Erro ao executar SQL:', '\n-----------\n', sql, '\n-----------\n', ex, '\n-----------\n')
            return False, 0, ex


    def sqlInject(self, item):
        return str(item).replace('\'', '\'\'')