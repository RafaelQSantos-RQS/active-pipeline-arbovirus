import os
import xmltodict
from logging import info, error
from  modules.entrez import esearch, efetch
from modules.utilities import data_atual

class Landing:
    '''
    '''
    @staticmethod
    def full_process(database:str, term:str, **kwargs):
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
        try:
            for tentativa in range(1,4):
                try:
                    info(f"Efetuando a requisição (Tentativa {tentativa})")
                    response_esearch = Landing._extract(database, term, kwargs)

                    info("Sucesso! Salvando a resposta!")
                    Landing._load(response_esearch)
                    
                    return None
                    
                except Exception as e:
                    info(f"Erro ao executar a etapa landing! -> {e}")
                    raise e
        except Exception as e:
            raise e

    @staticmethod
    def _load(response_esearch):
        with open(f"data/landing/{data_atual()}.xml",'wb') as xml_file:
            xml_file.write(response_esearch.content)

    @staticmethod
    def _extract(database, term, kwargs):
        response_esearch = esearch(database=database,term=term,**kwargs)
        return response_esearch
    
class Bronze:
    '''
    '''
    @staticmethod
    def full_process(database:str,**kwargs):
        '''
        '''
        try:
        
            info("Iniciando a etapa bronze.")
            xml_path, xml_dict = Bronze._extract()
            listas_para_a_requisição = Bronze._transform(xml_dict)
            Bronze._load(database, kwargs, xml_path, listas_para_a_requisição)
            info("Etapa bronze executada com sucesso!!")
        
        except Exception as e:
            error(f"Erro ao executar a etapa bronze -> {e}")
            raise e


    @staticmethod
    def _extract():
        '''
        '''
        info("Selecionando o XML mais novo para efetuar a transformação")
        landing_path = 'data/landing'
        lista_de_arquivos_da_landing = [os.path.join(landing_path,arquivo) for arquivo in os.listdir(landing_path)]
        xml_path = max(lista_de_arquivos_da_landing, key=os.path.getmtime)

        info(f"Lendo o xml {os.path.basename(xml_path)} da pasta landing.")
        with open(os.path.join(landing_path,f"{data_atual()}.xml"),'r') as xml_file:
            xml_data = xml_file.read()

        info("Transformando o xml em um dicionário Python.")
        xml_dict = xmltodict.parse(xml_data)
        return xml_path,xml_dict
    

    @staticmethod
    def _transform(xml_dict):
        '''
        '''
        info("Extraindo a lista de UID.")
        id_list = xml_dict.get('eSearchResult').get('IdList').get('Id')

        info("Quebrando a lista de ID em listas menores de até 200 IDs.")
        listas_para_a_requisição = []
        for i in range(0,len(id_list),200):
            listas_para_a_requisição.append(id_list[i:i+200])
        return listas_para_a_requisição
    
    @staticmethod
    def _load(database, kwargs, xml_path, listas_para_a_requisição):
        '''
        '''
        info("Efetuando as requisições.")
        for i in range(1,len(listas_para_a_requisição)+1):
            info(f"Efetuando a requisição {i}.")
            efetch_response = efetch(database=database,id=listas_para_a_requisição[i-1],**kwargs)

            info(f"Salvando a requisição {i}.")
            save_path = os.path.join(f'data/bronze/{os.path.basename(xml_path).replace(".",f" ({i}).")}')
            with open(save_path,'wb') as xml_file:
                xml_file.write(efetch_response.content)