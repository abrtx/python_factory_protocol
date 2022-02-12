import pandasql as ps

from protocol.Protocol import Ejecuta,ProcessFactory
from queryclass.Query import FechaFinMesProcess


class ExecFechaFinMes:
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
