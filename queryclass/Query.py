import pandas as pd
from dataclasses import dataclass

from pkgs.oracle import getArgs, openFile
from engine.conn import engine

@dataclass
class FechaFinMesProcess:
    """Fecha Fin de Mes:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("fecha_fin_mes")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self):
        print(pd.read_sql_query(self.open_query,self.engine.con_db()))


@dataclass
class MonedaCambioProcess:
    """Fecha Fin de Mes:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("moneda_cambio")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self):
        print(pd.read_sql_query(self.open_query,self.engine.con_db()))
