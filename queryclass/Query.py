import pandas as pd
from dataclasses import dataclass
import warnings
warnings.simplefilter("ignore")

from pkgs.oracle import getArgs, openFile
from engine.conn import engine

@dataclass
class FechaFinMesProcess:
    """Fecha Fin de Mes:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("fecha_fin_mes")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self): 
        fecha = (pd.read_sql_query(self.open_query,self.engine.con_db()))
        print(fecha)
        return fecha


@dataclass
class MonedaCambioProcess:
    """Fecha Fin de Mes:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("moneda_cambio")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self): 
        moneda = (pd.read_sql_query(self.open_query,self.engine.con_db()))
        print(moneda)
        return moneda

@dataclass
class PreciosSuperProcess:
    """Fecha Fin de Mes:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("precios_super")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self): 
        precio = (pd.read_sql_query(self.open_query,self.engine.con_db()))
        print(precio)
        return precio

@dataclass
class CuotasCierreProcess:
    """Fecha Fin de Mes:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("cuotas_cierre")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self): 
        cuota = (pd.read_sql_query(self.open_query,self.engine.con_db()))
        print(cuota)
        return cuota

@dataclass
class AjustesIngresadosProcess:
    """Ajustes Ingresados: inv_ajustes_movto:"""

    query: str = getArgs()
    opFile: str = openFile()
    engine: str = engine()

    @property
    def get_query(self):
        return self.query.putFullSql("ajustes_ingresados")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_query(self): 
        cuota = (pd.read_sql_query(self.open_query,self.engine.con_db()))
        print(cuota)
        return cuota

    
@dataclass
class SaldoPromesas:
    """Fecha Fin de Mes:"""
    query: str = getArgs()
    #saldos_promesas_0: str = ""

    @property
    def get_query(self):
        return self.query.putFullHoja("Promesas y Suscripciones CFI")

    def exec_inicio(self,i):
        self.saldos_promesas_0=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=10)
        print(self.saldos_promesas_0)
        return self.saldos_promesas_0
        
    def exec_CFIARESI_E(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=14)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0
    
    def exec_CFIPICAP_E(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=9)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0

    def exec_CFILVADV_E(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=23)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0

    def exec_CFIPICHL_E(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=11)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0


    def exec_CFITIN2A_E(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=10)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0


    def exec_CFICGPE4_E(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,124)),skipfooter=16)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0

        
    def exec_OTRO(self,i):
        cfi=pd.read_excel(self.get_query,sheet_name=i,header=2,usecols=list(range(1,12)),skiprows=list(range(3,123)),skipfooter=10)
        self.saldos_promesas_0=pd.concat([self.saldos_promesas_0,cfi])
        print(cfi)
        return self.saldos_promesas_0
        
@dataclass
class SaldoProm1:
    """Fecha Fin de Mes:"""
    query: str = getArgs()
    opFile: str = openFile()

    @property
    def get_query(self):
        return self.query.putFullSqldf("saldos_promesas_1")

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query


@dataclass
class SaldoProm2:
    """Fecha Fin de Mes:"""
    query: str = getArgs()
    opFile: str = openFile()
    saldos_promesas_0: str = ""
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('saldos_promesas_2')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query


@dataclass
class SaldoProm:
    """Fecha Fin de Mes:"""
    query: str = getArgs()
    opFile: str = openFile()
    saldos_promesas_0: str = ""
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('saldos_promesas')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query


@dataclass
class MonedasDistintas:
    """Trae monedas distintas desde saldo_promesas:"""
    query: str = getArgs()
    opFile: str = openFile()
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('monedas_distintas')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query


@dataclass
class CuotasNuevas:
    """Trae monedas distintas desde saldo_promesas:"""
    query: str = getArgs()
    opFile: str = openFile()
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('cuotas_nuevas')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query

@dataclass
class DiferenciaCuotasDetalle:
    """Trae monedas distintas desde saldo_promesas:"""
    query: str = getArgs()
    opFile: str = openFile()
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('diferencia_cuotas_detalle')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query


@dataclass
class NemosAjustesIngresados:
    """Trae monedas distintas desde saldo_promesas:"""
    query: str = getArgs()
    opFile: str = openFile()
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('nemos_ajustes_ingresados')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query


@dataclass
class AjustesIngresadosDetalle:
    """Trae monedas distintas desde saldo_promesas:"""
    query: str = getArgs()
    opFile: str = openFile()
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('ajustes_ingresados_detalle')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query

@dataclass
class DiferenciasIngreso:
    """Diferencias diferencia_cuotas_detalle, ajustes_ingresados_detalle"""
    query: str = getArgs()
    opFile: str = openFile()
    
    @property
    def get_query(self):
        return self.query.putFullSqldf('diferencias_ingreso')

    @property
    def open_query(self):
        return self.opFile.open_r(self.get_query).read()

    def exec_Saldos_Prom(self):
        return self.open_query
    
