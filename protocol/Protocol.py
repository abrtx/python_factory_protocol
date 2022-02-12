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
