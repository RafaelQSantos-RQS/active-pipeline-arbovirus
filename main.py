import modules.Extract.entrez as en
from logging import info, error, basicConfig, INFO
from modules.Util.files import prepare_data_filesystem
from modules.Util.files import extract_uidlist_from_xml

basicConfig(level=INFO, format=f'%(asctime)s: %(message)s',datefmt='%d/%m/%Y %H:%M:%S') # Configurando a forma com o log será printado

def main():
    print("\t\t### PREPARAÇÃO ###")
    prepare_data_filesystem()

    print("\t\t### EXTRAÇÃO ###")
    info("Efetuando a requisição da lista de UID que tenham Chikungunya no title e foi publicado em até 60")
    response_esearch = en.esearch(database="nucleotide",term="Chikungunya[title]",retmax=100,datetype="pdat",reldate=60)
    
    info("Extraindo a lista de UID do xml retornado")
    list_of_uid = extract_uidlist_from_xml(xml_bytes=response_esearch.content)
    
    response_efetch = en.efetch(database='nucleotide',id=list_of_uid,rettype='xml')
    with open('sequence.xml','wb') as xml_file:
        xml_file.write(response_efetch.content)

if __name__ == "__main__":
    try:
        
        info("Inicio do pipeline.")
        main()
        info("Fim do pipeline.")
    except Exception as ex:
        error(f"Erro ao executar o pipeline: {ex}")