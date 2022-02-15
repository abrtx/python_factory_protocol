from __future__ import annotations

import pandasql as ps
from dataclasses import dataclass

from protocol.Protocol import Ejecuta,ProcessFactory
from queryclass.Query import FechaFinMesProcess, MonedaCambioProcess


class ExecFechaFinMes:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return FechaFinMesProcess()

class ExecMonedaCambio:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return MonedaCambioProcess()


class Facade:
    def __init__(self):
        self.query0 = ExecFechaFinMes()
        self.query1 = ExecMonedaCambio()


    def operacion(self):
        query0 = self.query0.get_query_exec()
        query1 = self.query1.get_query_exec()

        results = []
        results.append(query0.exec_query())
        results.append(query1.exec_query())

        return results


# QUERIES = {
#     "fecha_fin_mes":ExecFechaFinMes(),
#     "moneda_cambio":ExecMonedaCambio()
#      # "precios_super":()
#      # "cuotas_cierre":()

#  }

# def read_process() -> ProcessFactory:
#     for clave, valor in QUERIES.items():
#         query = valor
#         query = list(QUERIES.values())[1]
#         return query


def do_process(facade: Facade):
    print(facade.operacion(), end = "")

# def do_query(fac: ProcessFactory):

#     get_script = fac.get_query_exec()

#     get_script.exec_query()





def main():

    #get_query = read_process()

    #do_query(get_query)
    facade = Facade()
    do_process(facade)

if __name__ == "__main__":
    main()
