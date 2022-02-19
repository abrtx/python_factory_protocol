from protocol.Protocol import *
from queryclass.Query import *
#from queryclass.Query import FechaFinMesProcess, MonedaCambioProcess, PreciosSuperProcess, CuotasCierreProcess, AjustesIngresadosProcess, SaldoPromesas, SaldoProm1, SaldoProm2, SaldoProm, MonedasDistintas, CuotasNuevas, DiferenciaCuotasDetalle, NemosAjustesIngresados, AjustesIngresadosDetalle, DiferenciasIngreso

class ExecFechaFinMes:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return FechaFinMesProcess()

class ExecMonedaCambio:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return MonedaCambioProcess()

class ExecPreciosSuper:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return PreciosSuperProcess()

class ExecCuotasCierre:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return CuotasCierreProcess()

class ExecAjustesIngresados:
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        return AjustesIngresadosProcess()


class ExecHoja:
    """Construye Proceso para ejecutar script"""

    def get_hoja_exec(self) -> EjecutaHoja:
        return SaldoPromesas()

class ExecSqldfSaldosProm1:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return SaldoProm1()

class ExecSqldfSaldosProm2:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return SaldoProm2()

class ExecSqldfSaldosProm:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return SaldoProm()

class ExecSqldfMonedasDistintas:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return MonedasDistintas()

class ExecSqldfCuotasNuevas:
    
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return CuotasNuevas()

class ExecSqldfDiferenciaCuotasDetalle:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return DiferenciaCuotasDetalle()
    
class ExecSqldfNemosAjustesIngresados:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return NemosAjustesIngresados()

class ExecSqldfAjustesIngresadosDetalle:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return AjustesIngresadosDetalle()

class ExecSqldfDiferenciasIngreso:
    """Construye Proceso para ejecutar script"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        return DiferenciasIngreso()
    
