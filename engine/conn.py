from dataclasses import dataclass
from pkgs.oracle import dbOra

@dataclass
class engine:
    dbora: str = dbOra()

    def con_db(self):
        eng = self.dbora.inversiones()
        return eng
