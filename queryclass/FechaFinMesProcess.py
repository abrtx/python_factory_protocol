import pandas as pd
from pkgs.oracle import dbOra, getArgs, openFile

class FechaFinMesProcess:
    """Fecha Fin de Mes:"""

    def __init__(self):
        self.query = getArgs()
        self.opFile = openFile()

    @property 
    def engine(self):
        dbora = dbOra()
        user = "inversiones"
        eng = dbora.inversiones()
        return eng

    @property
    def get_query(self):
        return self.query.putFullSql("fecha_fin_mes")

    @property
    def open_query(self):
        return str(self.opFile.open_r(self.get_query).read())

    def exec_query(self):
        print(pd.read_sql_query(self.open_query,self.engine))
