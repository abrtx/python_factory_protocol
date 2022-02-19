from dataclasses import dataclass

from protocol.Protocol import Ejecuta, ProcessFactory, ExecFechaFinMes, ExecMonedaCambio, ExecPreciosSuper, ExecCuotasCierre, ExecHoja, EjecutaHoja, ProcessHoja
from queryclass.Query import FechaFinMesProcess, MonedaCambioProcess, PreciosSuperProcess, CuotasCierreProcess, SaldoPromesas

@dataclass
class RunSql:
    query0: str = ExecFechaFinMes()
    query1: str = ExecMonedaCambio()
    query2: str = ExecPreciosSuper()
    query3: str = ExecCuotasCierre()    
    query4: str = ExecHoja()

    def operacion(self):
        query0 = self.query0.get_query_exec()
        query1 = self.query1.get_query_exec()
        query2 = self.query2.get_query_exec()
        query3 = self.query3.get_query_exec()        
        query4 = self.query4.get_hoja_exec()        

        results = []
        results.append(query0.exec_query())
        results.append(query1.exec_query())
        results.append(query2.exec_query())
        results.append(query3.exec_query())        
        results.append(query4.exec_inicio())
        results.append(query4.exec_CFIARESI_E())
        results.append(query4.exec_CFIPICAP_E())
        results.append(query4.exec_CFILVADV_E())
        results.append(query4.exec_CFIPICHL_E())
        results.append(query4.exec_CFITIN2A_E())
        results.append(query4.exec_CFICGPE4_E())
        
        return results


def do_process(sql: RunSql):
    print(sql.operacion(), end = "")
