import os
import xmltodict
import modules.api.entrez as entrez
from logging import info, error, basicConfig, INFO
from modules.utilities import data_atual
basicConfig(level=INFO, format=f'%(asctime)s: %(message)s',datefmt='%d/%m/%Y %H:%M:%S')

def bronze_step(database:str,**kwargs):
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

    info("Extraindo a lista de UID.")
    id_list = xml_dict.get('eSearchResult').get('IdList').get('Id')

    info("Quebrando a lista de ID em listas menores de até 200 IDs.")
    listas_para_a_requisição = []
    for i in range(0,len(id_list),200):
        listas_para_a_requisição.append(id_list[i:i+200])

    info("Efetuando as requisições.")
    for i in range(1,len(listas_para_a_requisição)+1):
        info(f"Efetuando a requisição {i}.")
        efetch_response = entrez.efetch(database=database,id=listas_para_a_requisição[i-1],**kwargs)

        info(f"Salvando a requisição {i}.")
        save_path = os.path.join(f'data/bronze/{os.path.basename(xml_path).replace(".",f" ({i}).")}')
        with open(save_path,'wb') as xml_file:
            xml_file.write(efetch_response.content)

