import pandas as pd
import pandasql as ps
from sqlalchemy import create_engine
from abc import ABC,abstractmethod
from typing import Protocol

from pkgs.oracle import dbOra, getArgs, openFile


class Ejecuta(ABC):
    """Ejecucion basica"""

    @abstractmethod
    def get_query(self):
        """Apunta ruta de query a ejecutar."""

    @abstractmethod
    def open_query(self):
        """Abre query en modo solo lectura."""

    @abstractmethod
    def exec_query(self):
        """Ejecuta script"""


class FechaFinMesProcess(Ejecuta):
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



class ProcessFactory(ABC):
    """Construye Proceso para ejecutar script"""

    @abstractmethod
    def get_query_exec(self) -> Ejecuta:
        """Metodo abstracto que trae a ejecucion una query"""

class ExecFechaFinMes(ProcessFactory):
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return FechaFinMesProcess()


QUERIES = {
    "fecha_fin_mes":(ExecFechaFinMes())
    # "moneda_cambio":()
    # "precios_super":()
    # "cuotas_cierre":()

}


def read_process() -> ProcessFactory:
    query = list(QUERIES.values())[0]
    return query


def do_query(fac: ProcessFactory):

    get_script = fac.get_query_exec()

    get_script.exec_query()



def main():

    get_query = read_process()

    do_query(get_query)

if __name__ == "__main__":
    main()
