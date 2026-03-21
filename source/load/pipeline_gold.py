from source.load.gold_herois_por_especies import GoldHeroisPorEspecies
from source.load.gold_top10_herois import GoldTop10Herois
from source.load.gold_top10_herois_femininos import GoldTop10HeroisFemininos 
from source.load.gold_top10_herois_masculinos import GoldTop10HeroisMasculinos


class GoldAtualizaRelatorios:
    def __init__(self):
        pass

    def executa_pipeline(self):
        GoldTop10HeroisMasculinos().executa_pipeline()
        GoldTop10HeroisFemininos().executa_pipeline()
        GoldTop10Herois().executa_pipeline()
        GoldHeroisPorEspecies().executa_pipeline()
