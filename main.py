import modules.entrez as entrez
from logging import info, error, basicConfig, INFO
from modules.utilities import prepare_data_filesystem,extract_uidlist_from_xml

basicConfig(level=INFO, format=f'%(asctime)s: %(message)s',datefmt='%d/%m/%Y %H:%M:%S') # Configurando a forma com o log será printado

def main():
    print("\t\t### PREPARAÇÃO ###")
    prepare_data_filesystem()

    
if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        error(f"Erro ao executar o pipeline: {ex}")