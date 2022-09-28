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
            new_list.append(new_dict)

        return json.dumps(new_list)

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
                        new_list.append(item)
                v = new_list

            if ':' in k:
                new_dict[k] = v
            elif k in ['RelatieObject.bron', 'RelatieObject.doel']:
                new_dict['https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#' + k] = v
            elif k not in ['@type', '@id']:
                new_dict[self.valid_uris[k]] = v
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
