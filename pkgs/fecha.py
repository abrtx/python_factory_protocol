import datetime
import time
from dataclasses import dataclass

@dataclass
class Fecha:
    fecha_actual: str = time.strftime("%Y%m%d")
    


if __name__ == "__main__":
    pass
#    fecha = Fecha.fecha_actual
#    print(fecha)
