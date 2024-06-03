import enum
Enum = enum.Enum
 
import json
 
import hashlib
 
from dagster import (multi_asset, AssetOut, AssetKey, AssetExecutionContext, MaterializeResult, EventRecordsFilter, DagsterEventType, Output)
 
import requests
 
import os
 
import geopandas as gpd
 
""" Criando os Enums para garantir que os valores computados como string sejam sempre os mesmos nas referencias;
    """
 
class Action(enum.Enum):
    DownloadCenso = "DownloadCenso"
    LoadCenso = "LoadCenso"
 
class Censo(enum.Enum):
    Censo2010 = "2010"
    Censo2022 = "2022"    
   
class GeoLevel(enum.Enum):
    Setores = "Setores"
    Distritos = "Distritos"
   
 
"""Agora vamos criar a classe gerenciadora de pacotes para comunicacao dentro do Pipeline"""
   
class Packet:
    def __init__(self, action : Action, *payloads) -> None:
        self.action = action
        self.payloads = payloads
 
    def __str__(self) -> str:
        serialize_dict = {"a": self.action.name}
        for i, payload in enumerate(self.payloads):
            if isinstance(payload, enum.Enum):
                serialize_dict[f'p{i}'] = payload.value
            else:
                serialize_dict[f'p{i}'] = payload
        return json.dumps(serialize_dict, separators=(',', ':'))
 
    def __bytes__(self) -> bytes:
        return str(self).encode("utf-8")
   
"""Aqui criaremos as classes auxiliares de Packet"""
   
class DownloadCensoPacket(Packet):
    def __init__(self, action: Action, censo: Censo, geo_level: GeoLevel):
        super().__init__(action, censo, geo_level)
       
class LoadCensoPacket(Packet):
    def __init__(self, action: Action, censo: Censo, geo_level: GeoLevel, path: str, hash: str):
        super().__init__(action, censo, geo_level, path, hash)
       
   
"""Agora vamos adicionar a funcao de desserializar os pacotes e contruir classes instanciadas no escopo global a partir dos mesmos"""
 
def from_json(json_str: str) -> Packet:
    obj_dict = json.loads(json_str)
    action = Action[obj_dict.get('a')]
    payloads = [obj_dict.get(f'p{i}') for i in range(len(obj_dict) - 1)]
   
    packet_classes = {
        Action.DownloadCenso: DownloadCensoPacket,
        Action.LoadCenso: LoadCensoPacket
    }
   
    constructor = packet_classes.get(action)
   
    if constructor:
        return constructor(action, *payloads)
    else:
        raise ValueError(f"No valid packet class found for action: {action}")
   
"""Agora precisamos baixar e retornar os arquivos do censo,
    Mas antes vamos definir algumas funcoes auxiliares:"""
   
   
   
def generate_download_censo_packets(context: AssetExecutionContext):
    packets = []    
    for censo in Censo:
        for geo_level in GeoLevel:
            for action in [Action.DownloadCenso]:
               
                packet = DownloadCensoPacket(action=action, censo=censo, geo_level=geo_level)
                context.log.info(f'Generated packet is: {packet}')
                packets.append(packet)    
    return packets
 
def generate_hash_for_downloaded_censo(data):
    hash_object = hashlib.sha256(data)
    hex_dig = hash_object.hexdigest()
    return hex_dig
 
 
def download_censo_by_packet(context: AssetExecutionContext,download_censo_packet:DownloadCensoPacket):
   
      if download_censo_packet.action == Action.DownloadCenso:
        data_packet = from_json(str(download_censo_packet))
        context.log.info(f'packet is: {data_packet.action}')
       
        if isinstance(data_packet, DownloadCensoPacket):
            censo = Censo(data_packet.payloads[0])
            geo_level = GeoLevel(data_packet.payloads[1])
           
            context.log.info(f"Censo: {censo}, Geo Level: {geo_level}")
           
 
            if censo == Censo.Censo2010:
                nivel_str = 'setores_censitarios' if geo_level == GeoLevel.Setores else GeoLevel.Distritos.value.lower()
                url = f'https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/sp/sp_{nivel_str}.zip'
            elif censo == Censo.Censo2022:
                sufixo = '_Distrito' if geo_level == GeoLevel.Distritos else ''
                url = f'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_preliminares/malha_com_atributos/{geo_level.value.lower()}/json/UF/SP/SP_Malha_Preliminar{sufixo}_2022.zip'
            else:
                context.log.error(f"Unsupported censo year: {censo}")
                return None
           
            os.makedirs("tutorial/data", exist_ok=True)
           
            cwd = os.path.abspath("tutorial/data")  
            file_name = f"censo_{censo.value}_{geo_level.value}.zip"
            file_path = f"{cwd}/{file_name}"            
                     
            if os.path.exists(path=file_path):
                context.log.info(f"File already stored as {file_path}")
                hash = generate_hash_for_downloaded_censo(file_path)
                packet = LoadCensoPacket(Action.LoadCenso, censo, geo_level, file_path, hash= hash)
                return packet
           
            context.log.info(f"Downloading file from {url}")
            response = requests.get(url)
            context.log.info(f"Response status code: {response.status_code}")
           
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                context.log.info(f"File stored as {file_path}")
                hash = generate_hash_for_downloaded_censo(file_path)
                packet = LoadCensoPacket(Action.LoadCenso, censo, geo_level, file_path, hash= hash)
                return packet
           
            else:
                context.log.info(f"Failed to download file from {url}")
                return None
       
        context.log.info(f"Invalid packet type: {type(data_packet)}")
        return None
   
 
"""Agora criamos nosso asset que gera os pacotes para download, e para cada um, faz o download e salva na pasta determinada."""
 
@multi_asset(
    outs={
        "censo2010_setores_zip": AssetOut(key=AssetKey("censo2010_setores_zip"), io_manager_key= "io_manager", is_required= False),
        "censo2010_distritos_zip": AssetOut(key=AssetKey("censo2010_distritos_zip"), io_manager_key= "io_manager", is_required= False),
        "censo2022_setores_zip": AssetOut(key=AssetKey("censo2022_setores_zip"), io_manager_key= "io_manager", is_required= False),
        "censo2022_distritos_zip": AssetOut(key=AssetKey("censo2022_distritos_zip"), io_manager_key= "io_manager", is_required= False),
    })
def download_censo_files(context: AssetExecutionContext):
   
   
   
    packets = generate_download_censo_packets(context)
    assets = []
    hashs = []
    for packet in packets:
        asset = download_censo_by_packet(download_censo_packet = packet, context= context)
        hash_code = asset.payloads[-1]
        assets.append(asset)
        hashs.append(hash_code)
    return Output(value = assets[0], output_name="censo2010_setores_zip", metadata={"hash_code": hashs[0]}), Output(value = assets[1], output_name="censo2010_distritos_zip", metadata={"hash_code": hashs[1]}), Output(value = assets[2], output_name="censo2022_setores_zip", metadata={"hash_code": hashs[2]}), Output(value = assets[3], output_name="censo2022_distritos_zip", metadata={"hash_code": hashs[3]})
 
"""Agora criamos o Asset que carrega esses dados em um GeoDataFrame"""
 
@multi_asset(
    outs={
        "censo2010_setores": AssetOut(key=AssetKey("censo2010_setores"), io_manager_key= "io_manager"),
        "censo2010_distritos": AssetOut(key=AssetKey("censo2010_distritos"), io_manager_key= "io_manager"),
        "censo2022_setores": AssetOut(key=AssetKey("censo2022_setores"), io_manager_key= "io_manager"),
        "censo2022_distritos": AssetOut(key=AssetKey("censo2022_distritos"), io_manager_key= "io_manager"),
    },
    internal_asset_deps={
        "censo2010_setores": {AssetKey("censo2010_setores_zip")},
        "censo2010_distritos": {AssetKey("censo2010_distritos_zip")},
        "censo2022_setores": {AssetKey("censo2022_setores_zip")},
        "censo2022_distritos": {AssetKey("censo2022_distritos_zip")},
    })
def load_censo_files(context: AssetExecutionContext, censo2010_setores_zip, censo2010_distritos_zip, censo2022_setores_zip, censo2022_distritos_zip):
    assets = [censo2010_setores_zip, censo2010_distritos_zip, censo2022_setores_zip, censo2022_distritos_zip]
    files = []
    for asset in assets:
        file = gpd.read_file(asset.payloads[-2])
        files.append(file)
       
    return files[0], files[1], files[2], files[3]