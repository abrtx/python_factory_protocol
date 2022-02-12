import pandas as pd
import pandasql as ps
import time
import datetime
import cx_Oracle
import os


conn=cx_Oracle.connect('inversiones/consulta@estudios')
c=conn.cursor()


fecha_actual=time.strftime("%d/%m/%y")
#fecha_actual='02/11/21'
dia=fecha_actual[0:2]
mes=fecha_actual[3:5]
año=fecha_actual[6:8]
fecha_actual_sql="20"+año+mes+dia
#fecha_actual_sql='20210601'
fecha_fin_mes=pd.read_sql_query("SELECT MAX(FECHA) AS FECHA FROM GRAL.CALENDARIO WHERE FECHA<TO_DATE("+fecha_actual_sql+",'YYYYMMDD') AND HABIL='S'",conn)
moneda_cambio=pd.read_sql_query("SELECT ID_MONEDA,VALOR_PESO FROM GRAL.MONEDA_CAMBIO WHERE FECHA=TO_DATE("+fecha_actual_sql+",'YYYYMMDD') UNION SELECT 'NO', 1 FROM DUAL UNION SELECT 'CLP', 1 FROM DUAL",conn)
precios_super=pd.read_sql_query("SELECT FECHA_ACTUALIZA,INSTRUMENTO,SERIE,MONEDA,PRECIO_DIA FROM PRECIO_SUPER WHERE FECHA_ACTUALIZA=TO_DATE("+fecha_actual_sql+",'YYYYMMDD') and INSTRUMENTO='PFI'",conn)
print(precios_super)

