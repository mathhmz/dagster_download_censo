import os
import requests
import geopandas as gpd
import enum
import json
from typing import Optional
from logging import Logger, getLogger
from dagster import asset, SourceAsset, AssetIn, AssetExecutionContext, MaterializeResult

class Action(enum.Enum):
    DownloadCenso = "DownloadCenso"
    
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
        Action.DownloadCenso: DownloadCensoPacket
    }
    
    constructor = packet_classes.get(action)
    
    if constructor:
        return constructor(action, *payloads)
    else:
        raise ValueError(f"No valid packet class found for action: {action}")

class DownloadCensoPacket(Packet):
    def __init__(self, action: Action, censo: Censo, geo_level: GeoLevel):
        super().__init__(action, censo, geo_level)

# Example usage
packet = DownloadCensoPacket(action=Action.DownloadCenso, censo=Censo.Censo2010, geo_level=GeoLevel.Setores)

@asset
def censo(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    censo_packet = packet
    
    if censo_packet.action == Action.DownloadCenso:
        data_packet = from_json(str(censo_packet))
        print(data_packet)
        context.log.info(f'packet is: {data_packet}')
        
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
                context.log.error(f"Unsupported census year: {censo}")
                return None
            
            context.log.info(f"Downloading file from {url}")
            
            os.makedirs("data", exist_ok=True)
            file_name = f"censo_{censo.value}_{geo_level.value}.zip"
            file_path = f"./data/{file_name}"
            
            if os.path.exists(file_path):
                context.log.info(f"File already stored as {file_path}")
                df = gpd.read_file(filename=file_path)
                return df
            
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                context.log.info(f"File stored as {file_path}")
                df = gpd.read_file(file_path)
                return df
            else:
                context.log.info(f"Failed to download file from {url}")
                return None
        
        context.log.info(f"Invalid packet type: {type(data_packet)}")
        return None