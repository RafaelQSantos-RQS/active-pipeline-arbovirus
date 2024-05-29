from modules.etl import Landing, Bronze, Silver
from logging import info, error, basicConfig, INFO
from modules.utilities import prepare_data_filesystem

basicConfig(level=INFO, format=f'%(asctime)s: %(message)s',datefmt='%d/%m/%Y %H:%M:%S') # Configurando a forma com o log será printado

def main():
    print("\t## PREPARAÇÃO ##")    
    prepare_data_filesystem()

    print("\t## ETAPA LANDING ##")
    Landing.full_process(database='nucleotide',term='Chikungunya',retmax=1000,datetype="pdat",reldate=60)

    print("\t## ETAPA BRONZE ##")
    Bronze.full_process('nucleotide',rettype='gbc',retmode='xml')

    print("\t## ETAPA SILVER ##")
    Silver.full_process()

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        error(f"Erro ao executar o pipeline: {ex}")