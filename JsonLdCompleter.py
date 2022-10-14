import json
import sqlite3
from pathlib import Path


class JsonLdCompleter:
    def __init__(self, otl_db_path: Path):
        self.valid_uris = self.get_otl_uris_from_db(otl_db_path)

    def transform_json_ld(self, assets_json_ld):
        asset_dict = json.loads(assets_json_ld)
        new_list = []
        for asset in asset_dict:
            new_dict = self.fix_dict(asset)
            new_dict = self.fix_geo(new_dict)
            new_list.append(new_dict)

        return json.dumps(new_list), len(new_list)

    def transform_if_http_value(self, value):
        if not isinstance(value, str):
            return value
        if value.startswith('http'):
            value = {"@id": value}
        return value

    def fix_dict(self, old_dict):
        new_dict = {}
        for k, v in old_dict.items():
            if isinstance(v, dict):
                v = self.fix_dict(v)
            elif isinstance(v, list):
                new_list = []
                for item in v:
                    if isinstance(item, dict):
                        new_list.append(self.fix_dict(item))
                    else:
                        new_list.append(self.transform_if_http_value(item))
                v = new_list

            if ':' in k:
                new_dict[k] = self.transform_if_http_value(v)
            elif k in ['RelatieObject.bron', 'RelatieObject.doel']:
                new_dict[
                    'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#' + k] = self.transform_if_http_value(
                    v)
            elif k not in ['@type', '@id']:
                new_dict[self.valid_uris[k]] = self.transform_if_http_value(v)
            else:
                new_dict[k] = v
        return new_dict

    @staticmethod
    def get_otl_uris_from_db(otl_db_path):
        con = sqlite3.connect(otl_db_path)
        cur = con.cursor()
        d = {}
        res = cur.execute("""
SELECT uri FROM OSLOAttributen 
UNION SELECT uri FROM OSLODatatypeComplexAttributen 
UNION SELECT uri FROM OSLODatatypePrimitiveAttributen 
UNION SELECT uri FROM OSLODatatypeUnionAttributen""")
        for row in res.fetchall():
            uri = row[0]
            if '#' in uri:
                short_uri = uri[uri.find('#') + 1:]
            else:
                short_uri = uri[uri.rfind('/') + 1:]
            d[short_uri] = uri

        con.close()
        return d

    def fix_geo(self, new_dict):
        if 'loc:Locatie.puntlocatie' in new_dict and 'loc:3Dpunt.puntgeometrie' in new_dict['loc:Locatie.puntlocatie']:
            coords = new_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']['loc:DtcCoord.lambert72']
            x = coords['loc:DtcCoordLambert72.xcoordinaat']
            y = coords['loc:DtcCoordLambert72.ycoordinaat']
            z = coords['loc:DtcCoordLambert72.zcoordinaat']
            new_dict['http://example.org/ApplicationSchema#hasExactGeometry'] = {
                "@type": "http://www.opengis.net/ont/sf#Point",
                "http://www.opengis.net/ont/geosparql#asWKT": {
                        "@value": f"<https://www.opengis.net/def/crs/EPSG/9.9.1/31370> Point({x} {y})",
                        "@type": "http://www.opengis.net/ont/geosparql#wktLiteral"
                    }}

        return new_dict

    #  "loc:Locatie.geometrie": "",

    # "geo:Geometrie.log": [
    #     {
    #       "geo:DtcLog.gaVersie": "GA_2.2.0",
    #       "geo:DtcLog.geometrie": {
    #         "geo:DtuGeometrie.punt": "POINT Z(167585.6 211223.1 0)"
