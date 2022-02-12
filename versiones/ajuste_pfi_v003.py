import pandas as pd
import pandasql as ps
from sqlalchemy import create_engine
from abc import ABC,abstractmethod
from typing import Protocol
from precios_sp_class.oracle import dbOra, getArgs, openFile
#from precios_sp_class.fecha import Fecha

class ProcessFactory(ABC):
    """ Traer, 
        Abrir, 
        Leer,
        Ejecutar,
        Mostrar
    """
    @abstractmethod
    def get_query(self):
        pass

    @abstractmethod
    def open_file(self, query: get_query):
        pass

    @abstractmethod
    def read_file(self, query: open_file):
        pass

    @abstractmethod
    def exec_query(self, query: read_file):
        pass
    

class FechaFinMesProcess(ProcessFactory):
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
        
    def get_query(self,Q):
        return self.query.putFullSql(Q)        

    def open_file(self,dato):
        return self.opFile.open_r(dato)

    def read_file(self, query):
        return query.read()

    def exec_query(self, query):
        return pd.read_sql_query(query,self.engine)



class QueryProcess:

    def __init__(self, query: ProcessFactory):
        self.query = query
    
    def process(self):
        self.query.get_query()
        self.query.open_file()
        self.query.read_file()
        self.query.exec_query()
        
    # def read_process() -> ProcessFactory:

    #     FACTORIES = {
    #         "fecha_fin_mes":(FechaFinMesProcess())
    #         # "moneda_cambio":()
    #         # "precios_super":()
    #         # "cuotas_cierre":()
            
    #     }

#def main(Proc: ProcessFactory) -> None:
    
    # query = getArgs()
    # opFile = openFile()
    # dbora = dbOra()
    # engine = dbora.inversiones()

    # f_f_m = query.putFullSql("fecha_fin_mes") # Get: fecha_fin_mes
    # m_c = query.putFullSql("moneda_cambio")
    # p_s = query.putFullSql("precios_super")   
    # c_c = query.putFullSql("cuotas_cierre")

    # sql_ffm = opFile.open_r(f_f_m) # Open: fecha_fin_mes
    # sql_mc = opFile.open_r(m_c)
    # sql_ps = opFile.open_r(p_s)
    # sql_cc = opFile.open_r(c_c)    

    # r_q_sql_ffm = sql_ffm.read() # Read: fecha_fin_mes
    # r_q_sql_mc = sql_mc.read()
    # r_q_sql_ps = sql_ps.read()
    # r_q_sql_cc = sql_cc.read()    

    # fecha_fin_mes = pd.read_sql_query(r_q_sql_ffm,engine) # Exec: fecha_fin_mes
    # moneda_cambio = pd.read_sql_query(r_q_sql_mc,engine)
    # precios_super = pd.read_sql_query(r_q_sql_ps,engine)
    # cuotas_cierre = pd.read_sql_query(r_q_sql_cc,engine)    

    # print(fecha_fin_mes) #fecha_fin_mes
    # print(moneda_cambio)
    # print(precios_super)
    # print(cuotas_cierre)    

    
if __name__ == "__main__":
    query = FechaFinMesProcess()
    ruta = query.get_query("fecha_fin_mes")
    op_file = query.open_file(ruta)
    read_file = query.read_file(op_file)
    fecha_fin_mes = query.exec_query(read_file)
    print(fecha_fin_mes)
