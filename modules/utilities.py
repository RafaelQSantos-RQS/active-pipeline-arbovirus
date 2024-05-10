import os
from typing import NoReturn,List
from logging import info, error
import xml.dom.minidom as minidom
from datetime import datetime


def prepare_data_filesystem() -> NoReturn:
    '''
    Descrição
    ---------
    Método para criar a estrutura de diretório para armazenamento de dados.

    Parametros
    ----------
    Não há parametros de entrada.

    Retorno
    -------
    Não há retorno.
    '''

    # Definindo a lista de diretórios a ser criado
    list_of_paths = ['data/landing','data/bronze','data/silver','data/gold']

    # Iterando sobre cada um dos diretórios e tentando o criar
    for path in list_of_paths:
        try:
            info(f"Criando o diretório {path}")
            os.makedirs(path)
            info(f"Diretório {path} criado com sucesso!")
        except FileExistsError:
            error(f"O diretório {path} já existe, nada será feito.")
        except Exception as ex:
            error(f"Erro inesperado ao tentar criar o diretório {path}: {ex}")

def data_atual():
    """
    Descrição
    ---------
    Retorna a data atual formatada como DD-MM-AAAA.

    Utiliza a função `datetime.today()` para obter a data atual e formata-a
    como string no formato DD-MM-AAAA (ex: "09-04-2024").

    Parametros
    ---------
    Não há parametros

    Retorno
    -------
        str: Data atual formatada como string ("DD-MM-AAAA").
    """
    return datetime.today().strftime("%d-%m-%Y")

def extract_uidlist_from_xml(xml_bytes: bytes) -> List[str]:
    '''
    Descrição
    ---------
    Extrai uma lista de UIDs (Identificadores Únicos) de bytes XML.

    Parâmetros
    ----------
    xml_bytes : bytes
        O conteúdo XML em bytes.

    Retorno
    -------
    List[str]
        Uma lista contendo os UIDs extraídos.
    '''
    try:
        info("Decodificando bytes XML")
        decoded_xml = xml_bytes.decode('utf-8')
        info("Analisando a string XML")
        dom_tree = minidom.parseString(decoded_xml)
        root = dom_tree.documentElement
        info("Extraindo lista de UIDs")
        list_of_ids = []
        for element in root.getElementsByTagName("Id"):
            list_of_ids.append(element.firstChild.data)
        info("Lista de UIDs extraída com sucesso!")
        return list_of_ids
    except Exception as e:
        error(f"Ocorreu um erro ao extrair a lista de UIDs: {e}")
        raise

def extract_pubmed_list(cell: list | dict) -> List[str]:
    """
    Extracts PubMed IDs from a cell containing INSDReference data and removes duplicates.

    Args:
        cell: The cell containing INSDReference data (list or single dictionary).

    Returns:
        list: A list of unique PubMed IDs.
    """

    pubmed_ids = set()
    references = cell.get('INSDReference', [])

    for reference in references if isinstance(references, list) else [references]:
        pubmed_id = reference.get('INSDReference_pubmed')
        if pubmed_id:
            pubmed_ids.add(pubmed_id)

    return list(pubmed_ids)


def extract_country(cell) -> str | list | None:
    """
    Extracts unique country names from a cell's 'INSDFeature' information.

    Args:
        cell (dict): A dictionary representing a cell in a dataframe. It is expected
                     to contain 'INSDFeature' information.

    Returns:
        list: A list containing unique country names extracted from the cell's
              'INSDFeature' information.
    """
    unique_countries = set()
    features = cell.get('INSDFeature', [])

    for feature in features if isinstance(features, list) else [features]:
        quals = feature.get('INSDFeature_quals', {}).get('INSDQualifier', [])

        for qual in quals if isinstance(quals, list) else [quals]:
            if qual.get('INSDQualifier_name') == 'country':
                unique_countries.add(qual.get('INSDQualifier_value'))

    if len(unique_countries) == 1:
        return unique_countries.pop()  # Retorna o único elemento do conjunto
    elif len(unique_countries) > 1:
        return list(unique_countries)
    else:
        return None

def extract_host(cell) -> str | list | None:
    unique_hosts = set()
    features = cell.get('INSDFeature', [])

    for feature in features if isinstance(features, list) else [features]:
        quals = feature.get('INSDFeature_quals', {}).get('INSDQualifier', [])

        for qual in quals if isinstance(quals, list) else [quals]:
            if qual.get('INSDQualifier_name') == 'host':
                unique_hosts.add(qual.get('INSDQualifier_value'))

    if len(unique_hosts) == 1:
        return unique_hosts.pop()  # Retorna o único elemento do conjunto
    elif len(unique_hosts) > 1:
        return list(unique_hosts)
    else:
        return None
    
def extract_collection_date(cell):
    collection_date = set()
    features = cell.get('INSDFeature', [])

    for feature in features if isinstance(features, list) else [features]:
        quals = feature.get('INSDFeature_quals', {}).get('INSDQualifier', [])

        for qual in quals if isinstance(quals, list) else [quals]:
            if qual.get('INSDQualifier_name') == 'collection_date':
                collection_date.add(qual.get('INSDQualifier_value'))

    if len(collection_date) == 1:
        return collection_date.pop()  # Retorna o único elemento do conjunto
    elif len(collection_date) > 1:
        return list(collection_date)
    else:
        return None
