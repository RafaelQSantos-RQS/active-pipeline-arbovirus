from logging import info, error
import modules.api.entrez as entrez
from modules.utilities import data_atual

def landing_step(database:str, term:str, **kwargs):
    """
    Descrição
    ---------
    Realiza a etapa de busca na NCBI e salva a resposta em um arquivo XML.

    Argumentos
    ----------
        database (str): Banco de dados da NCBI a ser utilizado (ex: "pubmed").
        term (str): Termo de busca na NCBI.
        **kwargs: Argumentos adicionais para a função entrez.esearch.

    Retorno
    -------
        None: Se a etapa for executada com sucesso.

    Exceções
    --------
        Exception: Se ocorrer algum erro durante a execução da etapa.
    """
    for tentativa in range(1,4):
        try:
            info(f"Efetuando a requisição (Tentativa {tentativa})")
            response_esearch = entrez.esearch(database=database,term=term,**kwargs)

            info("Sucesso! Salvando a resposta!")
            with open(f"data/landing/{data_atual()}.xml",'wb') as xml_file:
                xml_file.write(response_esearch.content)
            
            return None
            
        except Exception as e:
            info(f"Erro ao executar a etapa landing! -> {e}")
    
    raise Exception("Etapa não executada!")
    