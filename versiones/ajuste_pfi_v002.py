import pandas as pd
import pandasql as ps
from sqlalchemy import create_engine
from typing import Protocol
from precios_sp_class.oracle import dbOra, getArgs, openFile
#from precios_sp_class.fecha import Fecha


if __name__ == "__main__":    
    query = getArgs()
    opFile = openFile()
    dbora = dbOra()
    engine = dbora.inversiones()

    
    f_f_m = query.putFullSql("fecha_fin_mes") # Get: fecha_fin_mes
    m_c = query.putFullSql("moneda_cambio")
    p_s = query.putFullSql("precios_super")   
    c_c = query.putFullSql("cuotas_cierre")

    sql_ffm = opFile.open_r(f_f_m) # Open: fecha_fin_mes
    sql_mc = opFile.open_r(m_c)
    sql_ps = opFile.open_r(p_s)
    sql_cc = opFile.open_r(c_c)    

    r_q_sql_ffm = sql_ffm.read() # Read: fecha_fin_mes
    r_q_sql_mc = sql_mc.read()
    r_q_sql_ps = sql_ps.read()
    r_q_sql_cc = sql_cc.read()    

    fecha_fin_mes = pd.read_sql_query(r_q_sql_ffm,engine) # Exec: fecha_fin_mes
    moneda_cambio = pd.read_sql_query(r_q_sql_mc,engine)
    precios_super = pd.read_sql_query(r_q_sql_ps,engine)
    cuotas_cierre = pd.read_sql_query(r_q_sql_cc,engine)    

    print(fecha_fin_mes) #fecha_fin_mes
    print(moneda_cambio)
    print(precios_super)
    print(cuotas_cierre)    


