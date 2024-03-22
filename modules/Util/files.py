import os
from typing import NoReturn,List
from logging import info, error
import xml.dom.minidom as minidom


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
    list_of_paths = ['data/raw','data/analysis','data/processed']

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
