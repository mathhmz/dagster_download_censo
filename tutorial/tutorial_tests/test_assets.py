import unittest
from unittest.mock import patch, MagicMock
import requests
import geopandas as gpd
from tutorial.assets import Packet, Action, Censo, GeoLevel, DownloadCensoPacket, download_censo_by_packet, from_json

class TestPacket(unittest.TestCase):
    def test_packet_creation(self):
        packet = Packet(Action.DownloadCenso, Censo.Censo2010, GeoLevel.Setores)
        self.assertEqual(packet.action, Action.DownloadCenso)
        self.assertEqual(packet.payloads, (Censo.Censo2010, GeoLevel.Setores))

    def test_packet_str(self):
        packet = Packet(Action.DownloadCenso, Censo.Censo2010, GeoLevel.Setores)
        self.assertEqual(str(packet), '{"a":"DownloadCenso","p0":"2010","p1":"Setores"}')

    def test_packet_bytes(self):
        packet = Packet(Action.DownloadCenso, Censo.Censo2010, GeoLevel.Setores)
        self.assertEqual(bytes(packet), b'{"a":"DownloadCenso","p0":"2010","p1":"Setores"}')

class TestDownloadCenso(unittest.TestCase):
    @patch('tutorial.assets.requests.get')
    @patch('tutorial.assets.os.path.exists')
    @patch('tutorial.assets.gpd.read_file')
    def test_download_censo_by_packet(self, mock_read_file, mock_exists, mock_get):
        mock_context = MagicMock()
        mock_context.log = MagicMock()
        
        packet = DownloadCensoPacket(Action.DownloadCenso, Censo.Censo2010, GeoLevel.Setores)

        mock_exists.return_value = False
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b'Fake data'

        mock_read_file.return_value = gpd.GeoDataFrame()

        result = download_censo_by_packet(mock_context, packet)
        mock_context.log.info.assert_called()
        mock_get.assert_called()
        self.assertIsInstance(result, gpd.GeoDataFrame)

    @patch('tutorial.assets.requests.get')
    @patch('tutorial.assets.os.path.exists')
    @patch('tutorial.assets.gpd.read_file')
    def test_download_censo_by_packet_file_exists(self, mock_read_file, mock_exists, mock_get):
        mock_context = MagicMock()
        mock_context.log = MagicMock()
        
        packet = DownloadCensoPacket(Action.DownloadCenso, Censo.Censo2010, GeoLevel.Setores)

        mock_exists.return_value = True

        mock_read_file.return_value = gpd.GeoDataFrame()

        result = download_censo_by_packet(mock_context, packet)
        mock_context.log.info.assert_called()
        mock_exists.assert_called()
        self.assertIsInstance(result, gpd.GeoDataFrame)

if __name__ == '__main__':
    unittest.main()