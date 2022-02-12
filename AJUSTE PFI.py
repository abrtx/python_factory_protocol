import pandas as pd
import pandasql as ps
import time
import datetime
import cx_Oracle
import os
os.environ['PATH']='C:\\app\\client\\Oracle32\\product\\12.2.0\\client_1\\bin;C:\\Users\\controlinversiones\\Anaconda3\\DLLs;C:\\Users\\controlinversiones\\Anaconda3\\DLLs;C:\\Users\\controlinversiones\\Anaconda3;C:\\Users\\controlinversiones\\Anaconda3\\Library\\mingw-w64\\bin;C:\\Users\\controlinversiones\\Anaconda3\\Library\\usr\\bin;C:\\Users\\controlinversiones\\Anaconda3\\Library\\bin;C:\\Users\\controlinversiones\\Anaconda3\\Scripts;C:\\Users\\controlinversiones\\Anaconda3\\Library\\bin;O:\\oracle\\8isp\\bin;C:\\Program Files (x86)\\Oracle\\jre\\1.1.7\\bin\\;C:\\Windows\\SYSTEM32;C:\\Windows;C:\\Windows\\SYSTEM32\\WBEM;C:\\DMI\\WIN32\\BIN;O:\\ORA8I95\\BIN;O:\\PB5I32DK;O:\\SISTEMA\\INV;o:\\pb8dk;o:\\pb6dk'


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
cuotas_cierre=pd.read_sql_query('''
SELECT FECHA_VALORIZA,INSTRUMENTO,NEMO_BOLSA,ID_PERTENENCIA,ID_CUSTODIA,UNIDAD_ACTUAL,UNIDAD_ANTES,VALORIZA_ACTUAL,RUT_EMISOR FROM VAL_DETALLE WHERE INSTRUMENTO='PFI' AND FECHA_VALORIZA=(SELECT MAX(FECHA) AS FECHA FROM GRAL.CALENDARIO
WHERE FECHA<TO_DATE('''+fecha_actual_sql+''','YYYYMMDD') AND HABIL='S')
''',conn)
excel_saldos=pd.ExcelFile("Promesas y Suscripciones CFI.xlsx")
hojas=excel_saldos.sheet_names
hojas_cfi=hojas[2:len(hojas)-5]
#hojas_cfi.remove("CFIPK12F-E Contrato Malo")
#cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name="CFIPICHL-E",header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=14)
inicio=0

for i in hojas_cfi:
    if inicio==0:
        saldos_promesas_0=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=10)
        inicio=inicio+1
        print(saldos_promesas_0)
    else:
        if i=='CFIARESI-E':
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=14)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)

        elif i=='CFIPICAP-E':
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=9)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)

        elif i=='CFILVADV-E':
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=23)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)
            
        elif i=='CFIPICHL-E':
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=11)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)

        elif i=='CFITIN2A-E':
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=10)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)

        elif i=='CFICGPE4-E':
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,124)),skipfooter=16)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)
            
        else:
            cfi=pd.read_excel("Promesas y Suscripciones CFI.xlsx",sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=10)
            saldos_promesas_0=pd.concat([saldos_promesas_0,cfi])
            print(cfi)
    print(i)
saldos_promesas_1=ps.sqldf("SELECT *, 'PC'||substr(Nemotecnico,3,length(Nemotecnico)) AS NEMO_PFI,'PF'||substr(Nemotecnico,3,length(Nemotecnico)) AS NEMO_SUPER FROM saldos_promesas_0",locals())
saldos_promesas_2=ps.sqldf('''SELECT Nemotecnico,NEMO_PFI,NEMO_SUPER,CASE WHEN Nemotecnico='CFINHPTN-E' and s.Moneda='EUR' THEN 'US$' WHEN s.Moneda='USD' THEN 'US$' ELSE s.Moneda END Moneda,
CASE WHEN Nemotecnico='CFINHPTN-E' and s.Moneda='EUR'
then `Fondo A`*(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='EUR')/(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='US$') ELSE `Fondo A` END FONDO_A,
CASE WHEN Nemotecnico='CFINHPTN-E' and s.Moneda='EUR'
then `Fondo B`*(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='EUR')/(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='US$') ELSE `Fondo B` END FONDO_B,
CASE WHEN Nemotecnico='CFINHPTN-E' and s.Moneda='EUR'
then `Fondo C`*(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='EUR')/(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='US$') ELSE `Fondo C` END FONDO_C,
CASE WHEN Nemotecnico='CFINHPTN-E' and s.Moneda='EUR'
then `Fondo D`*(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='EUR')/(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='US$') ELSE `Fondo D` END FONDO_D,
CASE WHEN Nemotecnico='CFINHPTN-E' and s.Moneda='EUR'
then `Fondo E`*(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='EUR')/(SELECT VALOR_PESO FROM moneda_cambio where trim(ID_MONEDA)='US$') ELSE `Fondo E` END FONDO_E FROM saldos_promesas_1 s''',locals())
saldos_promesas=ps.sqldf('''SELECT Nemotecnico,NEMO_PFI,NEMO_SUPER,Moneda,SUM(FONDO_A) FONDO_A,SUM(FONDO_B) FONDO_B, SUM(FONDO_C) FONDO_C,SUM(FONDO_D) FONDO_D, SUM(FONDO_E) FONDO_E FROM saldos_promesas_2
group by Nemotecnico,NEMO_PFI,NEMO_SUPER,Moneda''')

monedas_distintas=ps.sqldf("SELECT NEMO_SUPER,s.Moneda,serie,p.MONEDA from saldos_promesas s left join precios_super p on trim(s.NEMO_SUPER)=trim(p.SERIE) where trim(s.Moneda)<>trim(p.MONEDA)")

cuotas_nuevas=ps.sqldf('''SELECT Nemotecnico,NEMO_PFI,NEMO_SUPER,s.Moneda,p.MONEDA as MONEDA_PRECIO,FONDO_A,FONDO_B,FONDO_C,FONDO_D,FONDO_E,round(FONDO_A/p.PRECIO_DIA*m1.VALOR_PESO/m2.VALOR_PESO,4) AS NUEVAS_A,
round(FONDO_B/p.PRECIO_DIA*m1.VALOR_PESO/m2.VALOR_PESO,4) AS NUEVAS_B,
round(FONDO_C/p.PRECIO_DIA*m1.VALOR_PESO/m2.VALOR_PESO,4) AS NUEVAS_C,round(FONDO_D/p.PRECIO_DIA*m1.VALOR_PESO/m2.VALOR_PESO,4) AS NUEVAS_D,round(FONDO_E/p.PRECIO_DIA*m1.VALOR_PESO/m2.VALOR_PESO,4) AS NUEVAS_E
FROM saldos_promesas s left join precios_super p on trim(s.NEMO_SUPER)=trim(p.SERIE) left join moneda_cambio m1 on trim(s.Moneda)=trim(m1.ID_MONEDA) left join moneda_cambio m2 on trim(p.MONEDA)=trim(m2.ID_MONEDA)
''')

diferencia_cuotas_detalle=ps.sqldf('''SELECT NEMO_PFI,NUEVAS_A - A.unidad_actual AS DIF_A,NUEVAS_B - B.unidad_actual AS DIF_B,NUEVAS_C - C.unidad_actual AS DIF_C,NUEVAS_D - D.unidad_actual AS DIF_D,
NUEVAS_E - ifnull(E.unidad_actual,0) AS DIF_E
from cuotas_nuevas s
left join (select nemo_bolsa,unidad_actual from cuotas_cierre where id_pertenencia=1) A on trim(A.NEMO_BOLSA)=trim(s.NEMO_PFI)
left join (select nemo_bolsa,unidad_actual from cuotas_cierre where id_pertenencia=2) B on trim(B.NEMO_BOLSA)=trim(s.NEMO_PFI)
left join (select nemo_bolsa,unidad_actual from cuotas_cierre where id_pertenencia=3) C on trim(C.NEMO_BOLSA)=trim(s.NEMO_PFI)
left join (select nemo_bolsa,unidad_actual from cuotas_cierre where id_pertenencia=4) D on trim(D.NEMO_BOLSA)=trim(s.NEMO_PFI)
left join (select nemo_bolsa,unidad_actual from cuotas_cierre where id_pertenencia=5) E on trim(E.NEMO_BOLSA)=trim(s.NEMO_PFI)
''',locals())

diferencia_cuotas_detalle.to_excel("ajuste detalle.xlsx")
diferencia_agregada=ps.sqldf("SELECT SUM(DIF_A) AS TOTAL_A,SUM(DIF_B) AS TOTAL_B,SUM(DIF_C) AS TOTAL_C,SUM(DIF_D) AS TOTAL_D,SUM(DIF_E) AS TOTAL_E FROM diferencia_cuotas_detalle",locals())

diferencia_agregada.to_excel("ajuste agregado.xlsx")

AJUSTES_INGRESADOS=pd.read_sql_query("select ID_PERTENENCIA,NEMO_BOLSA,CASE WHEN ID_MOVTO=11 THEN UNIDAD_NOMINAL*-1 ELSE UNIDAD_NOMINAL END UNIDADES from inv_ajustes_movto WHERE FECHA_MOVTO=TO_DATE("+fecha_actual_sql+",'YYYYMMDD')",conn)
if len(AJUSTES_INGRESADOS)>0:
    NEMOS_AJUSTES_INGRESADOS=ps.sqldf("SELECT DISTINCT(NEMO_BOLSA) NEMO_BOLSA FROM AJUSTES_INGRESADOS",locals())
    AJUSTES_INGRESADOS_DETALLE=ps.sqldf('''SELECT N.NEMO_BOLSA, FA.UNIDADES AS AJUSTE_A, FB.UNIDADES AS AJUSTE_B, FC.UNIDADES AS AJUSTE_C, FD.UNIDADES AS AJUSTE_D, FE.UNIDADES AS AJUSTE_E
                                        FROM NEMOS_AJUSTES_INGRESADOS N
                                        LEFT JOIN (SELECT * FROM AJUSTES_INGRESADOS WHERE ID_PERTENENCIA=1) FA ON N.NEMO_BOLSA=FA.NEMO_BOLSA
                                        LEFT JOIN (SELECT * FROM AJUSTES_INGRESADOS WHERE ID_PERTENENCIA=2) FB ON N.NEMO_BOLSA=FB.NEMO_BOLSA
                                        LEFT JOIN (SELECT * FROM AJUSTES_INGRESADOS WHERE ID_PERTENENCIA=3) FC ON N.NEMO_BOLSA=FC.NEMO_BOLSA
                                        LEFT JOIN (SELECT * FROM AJUSTES_INGRESADOS WHERE ID_PERTENENCIA=4) FD ON N.NEMO_BOLSA=FD.NEMO_BOLSA
                                        LEFT JOIN (SELECT * FROM AJUSTES_INGRESADOS WHERE ID_PERTENENCIA=5) FE ON N.NEMO_BOLSA=FE.NEMO_BOLSA''',locals())
    
    DIFERENCIAS_INGRESO=ps.sqldf("SELECT D.NEMO_PFI, IFNULL(DIF_A,0)-IFNULL(AJUSTE_A,0) AS DIFERENCIAS_INGRESO_A, IFNULL(DIF_B,0)-IFNULL(AJUSTE_B,0) AS DIFERENCIAS_INGRESO_B, IFNULL(DIF_C,0)-IFNULL(AJUSTE_C,0) AS DIFERENCIAS_INGRESO_C, IFNULL(DIF_D,0)-IFNULL(AJUSTE_D,0) AS DIFERENCIAS_INGRESO_D, IFNULL(DIF_E,0)-IFNULL(AJUSTE_E,0) AS DIFERENCIAS_INGRESO_E FROM diferencia_cuotas_detalle D LEFT JOIN AJUSTES_INGRESADOS_DETALLE AJ ON D.NEMO_PFI=AJ.NEMO_BOLSA",locals())
    DIFERENCIAS_INGRESO.to_excel("diferencias ingreso.xlsx")

