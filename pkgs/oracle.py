from sqlalchemy import create_engine
from dataclasses import dataclass
import os
from pathlib import Path

@dataclass
class dbOra:
    ora_conn_string: str = "oracle+cx_oracle://{username}:{password}@{hostname}/?encoding=UTF-8&nencoding=UTF-8"

    def prov_tre_14164377(self):
        engine = create_engine(
            self.ora_conn_string.format(
                username='PROV_TRE_141643770[EXP_LIMITES]',
                password='gxhmLOJv9KmXGUbN',
                hostname='DESAINV',
            )
        )
        return engine

    def inversiones(self):
        engine = create_engine(
            self.ora_conn_string.format(
                username='inversiones',
                password='consulta',
                hostname='estudios',
            )
        )
        return engine

@dataclass
class getArgs:
    dirname: str = os.path.dirname(__file__)
    sql: str = "oracle_pkg"

    def putFullSql(self, file_name):
        return f'{self.dirname}\{self.sql}\\'+file_name+'.sql'

    def putFullHoja(self, file_name):
        return f'{self.dirname}\{self.sql}\\'+file_name+'.xlsx'

    def putFullSqldf(self, file_name):
        return f'{self.dirname}\{self.sql}\\'+file_name+'.sqldf'


class openFile:
    def open_r(self,file):
        return open(file,'r')

    def open_w(self,file):
        return open(file,'w')

