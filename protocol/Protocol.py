from typing import Protocol

class Ejecuta(Protocol):
    """Ejecucion basica"""

    def get_query(self):
        """Apunta ruta de query a ejecutar."""

    def open_query(self):
        """Abre query en modo solo lectura."""

    def exec_query(self):
        """Ejecuta script"""


class ProcessFactory(Protocol):
    """Construye Proceso para ejecutar script"""

    def get_query_exec(self) -> Ejecuta:
        """Metodo abstracto que trae a ejecucion una query"""


class EjecutaHoja(Protocol):
    """Construye Proceso para leer hoja de calculo"""

    def exec_inicio(self):
        """Ejecuta lectura a hoja"""

    def exec_CFIARESI_E(self):
        """Ejecuta lectura a hoja"""

    def exec_CFIPICAP_E(self):
        """Ejecuta lectura a hoja"""

    def exec_CFILVADV_E(self):
        """Ejecuta lectura a hoja"""

    def exec_CFIPICHL_E(self):
        """Ejecuta lectura a hoja"""

    def exec_CFITIN2A_E(self):
        """Ejecuta lectura a hoja"""

    def exec_CFICGPE4_E(self):
        """Ejecuta lectura a hoja"""
        
class ProcessHoja(Protocol):
    """Realiza proceso para leer hoja"""

    def get_hoja_exec(self) -> EjecutaHoja:
        """trae a ejecucion hoja"""


class EjecutaSqldf(Protocol):
    """Construye Proceso para leer sqldf"""

    def exec_Saldos_Prom(self):
        """Ejecuta lectura sqldf"""


class ProcessSqldf(Protocol):
    """Realiza proceso para leer hoja"""

    def get_sqldf_exec(self) -> EjecutaSqldf:
        """trae a ejecucion hoja"""


