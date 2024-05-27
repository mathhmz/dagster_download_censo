import os
import requests
import geopandas as gpd
import enum
import json
from typing import Optional
from logging import Logger, getLogger
from dagster import asset, SourceAsset, AssetIn, AssetExecutionContext, MaterializeResult,multi_asset,AssetOut,AssetKey, FilesystemIOManager, Out
import zipfile
import io

class Action(enum.Enum):
    DownloadCenso = "DownloadCenso"
    LoadCenso = "LoadCenso"
    
class Censo(enum.Enum):
    Censo2010 = "2010"
    Censo2022 = "2022"    
    
class GeoLevel(enum.Enum):
    Setores = "Setores"
    Distritos = "Distritos" 

class Packet:
    def __init__(self, action, *payloads) -> None:
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
    


class DownloadCensoPacket(Packet):
    def __init__(self, action: Action, censo: Censo, geo_level: GeoLevel):
        super().__init__(action, censo, geo_level)
        
class LoadCensoPacket(Packet):
    def __init__(self, action: Action, censo: Censo, geo_level: GeoLevel, path: str):
        super().__init__(action, censo, geo_level, path)
        
        
def generate_download_packets(context: AssetExecutionContext):
    censos = [censo for censo in Censo]
    geo_levels = [geolevel for geolevel in GeoLevel]
    
    packets = []
    
    for censo in censos:
        for geo_level in geo_levels:
            for action in [Action.DownloadCenso]:
                packet = DownloadCensoPacket(action=action, censo=censo, geo_level=geo_level)
                context.log.info(f'Generated packet is: {packet}')
                packets.append(packet)         
    return packets


def prepare_file(file, bytes:bool = True):
    if bytes:
        file = zipfile.ZipFile(io.BytesIO(file))
    
          
def download_censo_by_packet(context: AssetExecutionContext,download_censo_packet:DownloadCensoPacket):
    
      if download_censo_packet.action == Action.DownloadCenso:
        data_packet = from_json(str(download_censo_packet))
        context.log.info(f'packet is: {data_packet.action}')
        
        
        
        if isinstance(data_packet, DownloadCensoPacket):
            censo = Censo(data_packet.payloads[0])
            geo_level = GeoLevel(data_packet.payloads[1])
            
            context.log.info(f"Censo: {censo}, Geo Level: {geo_level}")

            if censo == Censo.Censo2010:
                nivel_str = 'setores_censitarios' if geo_level == GeoLevel.Setores else None
                url = f'https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/sp/sp_{nivel_str}.zip'
            elif censo == Censo.Censo2022:
                sufixo = '_Distrito' if geo_level == GeoLevel.Distritos else ''
                url = f'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_preliminares/malha_com_atributos/{geo_level.value.lower()}/json/UF/SP/SP_Malha_Preliminar{sufixo}_2022.zip'
            else:
                context.log.error(f"Unsupported censo year: {censo}")
                return None
            
            context.log.info(f"Downloading file from {url}")
            
            os.makedirs("data", exist_ok=True)
            file_name = f"censo_{censo.value}_{geo_level.value}.zip"
            file_path = f"./data/{file_name}"
            packet = LoadCensoPacket(Action.LoadCenso, censo, geo_level, file_path)
            
            if os.path.exists(file_path):
                context.log.info(f"File already stored as {file_path}")
                censo = [None,packet]
                return censo
            
            response = requests.get(url)
            context.log.info(f"Response status code: {response.status_code}")
            
            if response.status_code == 200:

                censo [response.content, packet]
                return censo
            
            else:
                context.log.info(f"Failed to download file from {url}")
                return None
        
        context.log.info(f"Invalid packet type: {type(data_packet)}")
        return None
    
def load_censo_by_packet(context: AssetExecutionContext, load_censo_packet:LoadCensoPacket):

    if load_censo_packet.action == Action.LoadCenso:

        data_packet = from_json(str(load_censo_packet))

        if isinstance(data_packet, LoadCensoPacket):
            censo = Censo(data_packet.payloads[0])
            geo_level = GeoLevel(data_packet.payloads[1])
            file_path = data_packet.payloads[2]

            context.log.info(f"Loading file from {file_path}")

            if os.path.exists(file_path):
                context.log.info(f"File already stored as {file_path}")
                if zipfile.is_zipfile(file_path):
                    file_contents = {}
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        for file_name in zip_ref.namelist():
                            with zip_ref.open(file_name) as file:
                                file_contents[file_name] = file.read()
                    return file_contents
                else:
                    context.log.info(f"File is not a zip file: {file_path}")



            context.log.info(f"File not found: {file_path}")
            return None

        context.log.info(f"Invalid packet type: {type(data_packet)}")
        return None
    
@multi_asset(
    outs={
        "censo2010_setores_zip": AssetOut(key=AssetKey("censo2010_setores_zip"), io_manager_key= "io_manager"),
        "censo2010_distritos_zip": AssetOut(key=AssetKey("censo2010_distritos_zip"), io_manager_key= "io_manager"),
        "censo2022_setores_zip": AssetOut(key=AssetKey("censo2022_setores_zip"), io_manager_key= "io_manager"),
        "censo2022_distritos_zip": AssetOut(key=AssetKey("censo2022_distritos_zip"), io_manager_key= "io_manager"),
    } )
def download_censo_files(context: AssetExecutionContext):
    packets = generate_download_packets(context)
    assets = []
    for packet in packets:
        asset = download_censo_by_packet(download_censo_packet = packet, context= context)

        assets.append(asset)
    return assets[0], assets[1], assets[2], assets[3]

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
def load_censo_files(censo2010_setores_zip, censo2010_distritos_zip, censo2022_setores_zip, censo2022_distritos_zip):
    args = [censo2010_setores_zip, censo2010_distritos_zip, censo2022_setores_zip, censo2022_distritos_zip]
    print(args)
    packets = []
    assets = []
    for arg in args:
        packet = arg[1]
        packets.append(packet)
    
    for packet in packets:
        asset = load_censo_by_packet(load_censo_packet=packet, context=AssetExecutionContext)
        assets.append(asset)
        
    return assets[0], assets[1], assets[2], assets[3]
    
    
    
    
    
  