import pandas as pd
import pandasql as ps
from dataclasses import dataclass

from protocol.Ejecuta import *


@dataclass
class RunSql:
    query0: str = ExecFechaFinMes()
    query1: str = ExecMonedaCambio()
    query2: str = ExecPreciosSuper()
    query3: str = ExecCuotasCierre()    
    query4: str = ExecHoja()
    query5: str = ExecSqldfSaldosProm1()
    query6: str = ExecSqldfSaldosProm2()
    query7: str = ExecSqldfSaldosProm()
    query8: str = ExecSqldfMonedasDistintas()
    query9: str = ExecSqldfCuotasNuevas()
    query10: str = ExecSqldfDiferenciaCuotasDetalle()    
    query11: str = ExecAjustesIngresados()
    query12: str = ExecSqldfNemosAjustesIngresados()
    query13: str = ExecSqldfAjustesIngresadosDetalle()
    query14: str = ExecSqldfDiferenciasIngreso()
    
    def operacion(self):

        excel_saldos=pd.ExcelFile("Promesas y Suscripciones CFI.xlsx")
        hojas=excel_saldos.sheet_names
        hojas_cfi=hojas[2:len(hojas)-5]
        
        query0 = self.query0.get_query_exec()
        query1 = self.query1.get_query_exec()
        query2 = self.query2.get_query_exec()
        query3 = self.query3.get_query_exec()        
        query4 = self.query4.get_hoja_exec()
        query5 = self.query5.get_sqldf_exec()
        query6 = self.query6.get_sqldf_exec()
        query7 = self.query7.get_sqldf_exec()
        query8 = self.query8.get_sqldf_exec()
        query9 = self.query9.get_sqldf_exec() 
        query10 = self.query10.get_sqldf_exec()
        query11 = self.query11.get_query_exec()        
        query12 = self.query12.get_sqldf_exec()
        query13 = self.query13.get_sqldf_exec()
        query14 = self.query14.get_sqldf_exec()
        
        fecha_fin_mes = query0.exec_query()
        moneda_cambio = query1.exec_query() 
        precios_super = query2.exec_query()  
        cuotas_cierre = query3.exec_query()
        
        inicio=0
        for i in hojas_cfi:
            if inicio==0:
                saldos_promesas_0 = query4.exec_inicio(i)
                inicio=inicio+1
            else:
                if i=='CFIARESI-E':
                    saldos_promesas_0 = query4.exec_CFIARESI_E(i)
                elif i=='CFIPICAP-E':
                    saldos_promesas_0 = query4.exec_CFIPICAP_E(i)
                elif i=='CFILVADV-E':
                    saldos_promesas_0 = query4.exec_CFILVADV_E(i)
                elif i=='CFIPICHL-E':
                    saldos_promesas_0 = query4.exec_CFIPICHL_E(i)
                elif i=='CFITIN2A-E':
                    saldos_promesas_0 = query4.exec_CFITIN2A_E(i)
                elif i=='CFICGPE4-E':
                    saldos_promesas_0 = query4.exec_CFICGPE4_E(i)
                else:
                    saldos_promesas_0 = query4.exec_OTRO(i)
        print(i)
        saldos_promesas_1 = ps.sqldf(query5.exec_Saldos_Prom(),locals())
        print(saldos_promesas_1)
        saldos_promesas_2 = ps.sqldf(query6.exec_Saldos_Prom(),locals())
        print(saldos_promesas_2)
        saldos_promesas = ps.sqldf(query7.exec_Saldos_Prom(),locals())
        print(saldos_promesas)
        monedas_distintas = ps.sqldf(query8.exec_Saldos_Prom(),locals())
        print(monedas_distintas)
        cuotas_nuevas = ps.sqldf(query9.exec_Saldos_Prom(),locals())
        print(cuotas_nuevas)
        diferencia_cuotas_detalle = ps.sqldf(query10.exec_Saldos_Prom(),locals())
        print(diferencia_cuotas_detalle)
        diferencia_cuotas_detalle.to_excel("ajuste agregado.xlsx")
        ajustes_ingresados = query11.exec_query()
        print(ajustes_ingresados)

        if len(ajustes_ingresados)>0:
                    nemos_ajustes_ingresados = ps.sqldf(query12.exec_Saldos_Prom(),locals())
                    print(nemos_ajustes_ingresados)
                    ajustes_ingresados_detalle = ps.sqldf(query13.exec_Saldos_Prom(),locals())
                    print(ajustes_ingresados_detalle)
                    diferencias_ingreso = ps.sqldf(query14.exec_Saldos_Prom(),locals())
                    print(diferencias_ingreso)
                    diferencias_ingreso.to_excel("diferencias ingreso.xlsx")



def do_process(sql: RunSql):
    print(sql.operacion(), end = "")



def main():

    runsql = RunSql()
    do_process(runsql)

if __name__ == "__main__":
    main()
