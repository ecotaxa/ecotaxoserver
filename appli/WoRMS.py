# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2020  Picheral, Colin, Irisson (UPMC-CNRS)
#
# Wrapper for using http://www.marinespecies.org/index.php and its REST services
#
from typing import Dict, Tuple, List, Any
import requests

class WoRMSFinder(object):
    """
    A utility for finding in WoRMS service the equivalent of a given entry in Taxonomy.
    """

    BASE_URL = "https://www.marinespecies.org/rest/"
    the_session = None

    WoRMS_URL_AphiaRecordByName = "AphiaRecordsByName/%s?marine_only=true"
    WoRMS_URL_AphiaRecordsByIds ="AphiaRecordsByAphiaIDs?aphiaids=%s"
    WoRMS_URL_ClassifByAphia = "AphiaClassificationByAphiaID/%d"
    WoRMS_URL_ClassifChildrenByAphia = "AphiaChildrenByAphiaID/%d?marine_only=false&offset=%d"

    @classmethod
    def aphia_records_by_name_sync(cls, name: str) -> List[Dict]:  # pragma:nocover
        ret: List[Dict] = []
        session = cls.get_session()
        req = cls.WoRMS_URL_AphiaRecordByName % name
        response = session.get(cls.BASE_URL + req)
        if not response.ok:
            cls.invalidate_session()
        else:
            if response.status_code == 204:  # No content
                pass
            else:
                ret = response.json()
        return ret

    @classmethod
    def aphia_classif_by_id(cls, aphia_id: int, flatten=False) -> List[Dict]:
        """ Return basic information in lineage odrer, from Biota to requested"""
        req = cls.WoRMS_URL_ClassifByAphia % aphia_id
        session = cls.get_session()
        response = session.get(cls.BASE_URL + req)
        if not response.ok:
            return []
        else:
            rsp = response.json()
        # The response is a nested structure, flatten it
        if not flatten:
            return rsp
        ret = []
        while True:
            ret.append({k: v for k,v in rsp.items() if k != "child"})
            child = rsp.get('child')
            if child is None:
                break
            rsp = child
        return ret

    CHUNK_SIZE = 50

    @classmethod
    def aphia_children_by_id(
        cls, aphia_id: int, page=0
    ) -> Tuple[List[Dict], int]:  # pragma:nocover
        res: List[Dict] = []
        chunk_num = page * cls.CHUNK_SIZE + 1
        req = cls.WoRMS_URL_ClassifChildrenByAphia % (aphia_id, chunk_num)
        nb_queries = 1
        # try:
        session = cls.get_session()
        response = session.get(cls.BASE_URL + req)
        # Seen: httpcore._exceptions.ProtocolError: can't handle event type ConnectionClosed
        # when role=SERVER and state=SEND_RESPONSE
        # except ProtocolError as e:
        #     raise HTTP_X_Error("%s trying %s" % (e, req), request=req)
        if response.status_code == 204:
            # No content
            pass
        elif response.status_code == 200:
            res = response.json()
            if len(res) == cls.CHUNK_SIZE:
                next_page, cont_queries = cls.aphia_children_by_id(
                    aphia_id, page + 1
                )
                res.extend(next_page)
                nb_queries += cont_queries
        # else:
        #     raise HTTP_X_Error("%d trying %s" % (response.status_code, req), request=req)
        return res, nb_queries

    @classmethod
    def aphia_records_by_aphiaids(cls,aphiaids:List[int], page=0)->Tuple[List[Dict],int]:
        res: List[Dict] = []
        chunk_num = page * cls.CHUNK_SIZE + 1
        req = cls.WoRMS_URL_AphiaRecordsByIds  % (",".join(aphiaids[chunk_num:cls.CHUNK_SIZE]))
        nb_queries = 1
        session = cls.get_session()
        response = session.get(cls.BASE_URL + req)
        if response.status_code == 204:
            # No content
            pass
        elif response.status_code == 200:
            res = response.json()
            if len(res) == cls.CHUNK_SIZE:
                next_page, cont_queries = cls.aphia_records_by_aphiaids(
                aphiaids, page + 1
                )
                res.extend(next_page)
                nb_queries += cont_queries
        return res, nb_queries

    @classmethod
    def get_session(cls):
        """Cache the session to marinespecies.org, for speed and saving resources"""
        session = cls.the_session
        if session is None:
            session = requests.Session()
            cls.the_session = session
        return cls.the_session

    @classmethod
    def invalidate_session(cls):
        cls.the_session = None

    @staticmethod
    def reverse_lineage(lineage: dict):
        keys = list(lineage.keys())
        if 'child' in keys:
            keys.remove('child')
        flipped:dict = {}
        flipped = WoRMSFinder.get_lineage(lineage, keys, flipped)
        rev = dict(reversed(list(flipped.items())))
        return rev

    @staticmethod
    def get_lineage(lineage: dict,keys:list, flipped: dict):
        child = lineage['child']
        parent = {}
        for k in keys:
            parent.update({k: lineage[k]})
            flipped.update({str(child['AphiaID']): parent})
        if child['child'] is not None:
            flipped=WoRMSFinder.get_lineage(child, keys, flipped)
        return flipped
