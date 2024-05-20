import os
import xmltodict
import pandas as pd
from logging import info, error
from modules.utilities import data_atual
from  modules.entrez import esearch, efetch
from modules.pubmed import extract_pmcid_from_list
from modules.utilities import extract_pubmed_list, extract_country, extract_collection_date

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
                    response_esearch = Landing.__extract(database, term, kwargs)

                    info("Sucesso! Salvando a resposta!")
                    Landing.__load(response_esearch)
                    
                    return None
                    
                except Exception as e:
                    info(f"Erro ao executar a etapa landing! -> {e}")
                    raise e
        except Exception as e:
            raise e

    @staticmethod
    def __load(response_esearch):
        with open(f"data/landing/{data_atual()}.xml",'wb') as xml_file:
            xml_file.write(response_esearch.content)

    @staticmethod
    def __extract(database, term, kwargs):
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
            xml_path, xml_dict = Bronze.__extract()
            listas_para_a_requisição = Bronze.__transform(xml_dict)
            Bronze.__load(database, kwargs, xml_path, listas_para_a_requisição)
            info("Etapa bronze executada com sucesso!!")
        
        except Exception as e:
            error(f"Erro ao executar a etapa bronze -> {e}")
            raise e


    @staticmethod
    def __extract():
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
    def __transform(xml_dict):
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
    def __load(database, kwargs, xml_path, listas_para_a_requisição):
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

class Silver:
    '''
    '''
    @staticmethod
    def full_process():
        '''
        '''
        try:
            dataframe = Silver.__extract()
            transformed_datafreme = Silver.__transform(dataframe=dataframe)
        except Exception as e:
            raise e

    @staticmethod
    def __extract() -> pd.DataFrame:
        bronze_path = 'data/bronze'
        lista_de_xmls = [os.path.join(bronze_path,file) for file in os.listdir(bronze_path)]

        full_df = pd.DataFrame()
        for xml in lista_de_xmls:
            with open(xml,'r') as xml_file:
                xml_data = xml_file.read()
            full_df = pd.concat([full_df,pd.DataFrame(xmltodict.parse(xml_data).get('INSDSet').get('INSDSeq'))],ignore_index=True)

        return full_df

    @staticmethod
    def __transform(dataframe:pd.DataFrame) -> pd.DataFrame:
        full_df = dataframe[['INSDSeq_locus','INSDSeq_length','INSDSeq_update-date','INSDSeq_create-date','INSDSeq_references','INSDSeq_feature-table','INSDSeq_sequence']]

        # Renomeando colunas
        colunas_a_serem_renomeadas = {
            'INSDSeq_locus': 'locus',
            'INSDSeq_length':'length',
            'INSDSeq_update-date': 'update_date',
            'INSDSeq_create-date': 'create_date',
            'INSDSeq_sequence':'sequence'
            }
        full_df = full_df.rename(columns=colunas_a_serem_renomeadas) 

        # Alterar tipo das colunas
        colunas_a_serem_convertidas = {
            'length': 'int64',
            'update_date':'datetime64[ns]',
            'create_date':'datetime64[ns]'
            }
        full_df = full_df.astype(colunas_a_serem_convertidas)

        # Extraindo os pubmeds
        column = 'INSDSeq_references'
        full_df['pubmed'] = full_df[column].apply(lambda x: extract_pubmed_list(cell=x))
        full_df.drop(columns=column,inplace=True)

        # Extraindo os countrys e data de coleta
        column = 'INSDSeq_feature-table'
        full_df['country'] = full_df[column].apply(lambda x: extract_country(cell=x)) 
        full_df['collection_date'] = full_df['INSDSeq_feature-table'].apply(lambda x: extract_collection_date(cell=x))
        full_df.drop(columns=column, inplace=True)

        # Explodindo a coluna pubmed
        full_df = full_df.explode(column='pubmed')

        # Extrai os PMCID para todos os números Pubmed em paralelo
        list_of_pubmeds = full_df['pubmed'].drop_duplicates().dropna().tolist()
        pmcid_results = extract_pmcid_from_list(list_of_pubmed=list_of_pubmeds, number_of_webscrappers=4)

        # Cria um dicionário a partir dos resultados
        pmcid_dict = {temp_dict['pubmed_accession_number']: temp_dict['pmcid'] for temp_dict in pmcid_results}

        # Mapeia os PMCID de volta para o DataFrame
        full_df['pmcid'] = full_df['pubmed'].map(pmcid_dict)

        return full_df