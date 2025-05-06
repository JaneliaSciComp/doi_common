''' doi_common.py
    Library of routines for parsing and interpreting DOI/ORCID records.
    Callable read functions:
      get_abstract
      get_affiliations
      get_author_counts
      get_author_details
      get_author_list
      get_doi_record
      get_dois_by_author
      get_first_last_author_payload
      get_journal
      get_name_combinations
      get_project_map
      get_projects_from_dois
      get_publishing_date
      get_single_author_details
      get_supervisory_orgs
      get_title
      is_datacite
      is_janelia_author
      is_journal
      is_preprint
      short_citation
      single_orcid_lookup
    Callable write functions:
      add_doi_to_process
      add_orcid
      add_orcid_name
      update_existing_orcid
      update_jrc_fields
      update_jrc_author_from_doi
'''

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

from datetime import datetime
import logging
import os
import re
import requests
import jrc_common.jrc_common as JRC

DIS_URL = "https://dis.int.janelia.org/"
JANELIA_ROR = "013sk6x84"
ORCID_LOGO = "https://dis.int.janelia.org/static/images/ORCID-iD_icon_16x16.png"
ORGS_URL = "https://services.hhmi.org/IT/WD-hcm/supervisoryorgs"

# Logger
LLOGGER = logging.getLogger(__name__)

# ******************************************************************************
# * Internal functions                                                         *
# ******************************************************************************

def _adjust_payload(payload, row):
    ''' Update author detail with additional attributes
        Keyword arguments:
          payload: data record from author detail
          row: row from orcid collection
        Returns:
          None
    '''
    if row:
        payload['orcid'] = row['orcid'] if 'orcid' in row else ''
        payload['in_database'] = True
        if 'employeeId' in row:
            payload['validated'] = True
        payload['janelian'] = bool('alumni' not in row)
        if 'alumni' in row:
            payload['alumni'] = True
        if 'group' in row:
            payload['group'] = row['group']
        if 'group_code' in row:
            payload['group_code'] = row['group_code']
        if 'affiliations' in row:
            payload['tags'] = row['affiliations']
        if 'employeeId' in row and row['employeeId']:
            payload['employeeId'] = row['employeeId']
        if 'userIdO365' in row and row['userIdO365']:
            payload['userIdO365'] = row['userIdO365']
        if 'workerType' in row and row['workerType']:
            payload['workerType'] = row['workerType']


def _add_single_author_jrc(payload, coll):
    ''' Update groups and affiliations in author detail
        Keyword arguments:
          payload: data record from author detail
          coll: orcid collection
        Returns:
          None
    '''
    payload['in_database'] = False
    payload['janelian'] = False
    payload['asserted'] = False
    payload['alumni'] = False
    payload['validated'] = False
    payload['match'] = None
    if 'orcid' in payload:
        try:
            cnt = coll.count_documents({"given": payload['given'], "family": payload['family']})
            row = coll.find_one({"orcid": payload['orcid']})
        except Exception as err:
            raise err
        if row:
            payload['match'] = 'ORCID'
            if cnt > 1:
                payload['duplicate_name'] = True
        _adjust_payload(payload, row)
    if 'family' in payload and payload['match'] is None:
        try:
            cnt = coll.count_documents({"given": payload['given'], "family": payload['family']})
            row = coll.find_one({"given": payload['given'], "family": payload['family']})
        except Exception as err:
            raise err
        if row:
            payload['match'] = 'name'
            if cnt > 1:
                payload['duplicate_name'] = True
        _adjust_payload(payload, row)
    if 'affiliations' in payload and payload['affiliations']:
        # This is the "gold standard" for author matching
        for aff in payload['affiliations']:
            if 'Janelia' in aff:
                payload['janelian'] = True
                payload['asserted'] = True
                if payload['match'] != 'ORCID':
                    payload['match'] = 'asserted'
                _adjust_payload(payload, row)
                break


def _process_middle_initials(rec):
    ''' Add name combinations for first names in the forms "F. M." or "F."
        with or without the periods.
        Keyword arguments:
          rec: orcid collection record
        Returns:
          None
    '''
    for first in rec['given']:
        if re.search(r"[A-Za-z]\. [A-Za-z]\.$", first):
            continue
        if re.search(r"[A-Za-z]\.[A-Za-z]\.$", first):
            new = first.replace('.', ' ')
            if new not in rec['given']:
                rec['given'].append(new)
        elif re.search(r" [A-Za-z]\.$", first):
            new = first.rstrip('.')
            if new not in rec['given']:
                rec['given'].append(new)


def _set_paper_orcid(auth, datacite, payload):
    ''' Add an author's ORCID as specified in the paper
        Keyword arguments:
          auth: paper author record
          datacite: True if DataCite record
          payload: author detail record
        Returns:
          None
    '''
    if datacite:
        if 'nameIdentifiers' in auth:
            for nid in auth['nameIdentifiers']:
                if 'nameIdentifier' in nid and 'nameIdentifierScheme' in nid:
                    if nid['nameIdentifierScheme'] == 'ORCID':
                        payload['paper_orcid'] = nid['nameIdentifier'].split("/")[-1]
                        payload['orcid'] = payload['paper_orcid']
                        break
    elif 'ORCID' in auth:
        payload['paper_orcid'] = auth['ORCID'].split("/")[-1]

# ******************************************************************************
# * Callable read functions                                                    *
# ******************************************************************************

def get_abstract(rec):
    ''' Generate an abstract
        Keyword arguments:
          rec: data record
        Returns:
          Abstract
    '''
    if 'DOI' in rec:
        if 'abstract' in rec:
            return rec['abstract']
    elif 'descriptions' in rec:
        for desc in rec['descriptions']:
            if 'descriptionType' in desc and desc['descriptionType'] == 'Abstract' \
               and 'description' in desc:
                return desc['description']
    return None


def get_affiliations(idrec, rec):
    ''' Add affiliations
        Keyword arguments:
          idrec: record from HHMI's People service
          rec: orcid collection record
        Returns:
          None
    '''
    if idrec:
        if 'affiliations' in idrec and idrec['affiliations']:
            rec['affiliations'] = []
            for aff in idrec['affiliations']:
                if aff['supOrgName'] not in rec['affiliations']:
                    rec['affiliations'].append(aff['supOrgName'])
        # Add ccDescr
        if 'group' not in rec and 'ccDescr' in idrec and idrec['ccDescr']:
            if 'affiliations' not in rec:
                rec['affiliations'] = []
            if idrec['ccDescr'] not in rec['affiliations']:
                rec['affiliations'].append(idrec['ccDescr'])
        # Add supOrgName as a fallback
        if 'supOrgName' in idrec and idrec['supOrgName'] and 'affiliations' not in rec:
            if 'affiliations' not in rec:
                rec['affiliations'] = []
            if idrec['supOrgName'] not in rec['affiliations']:
                rec['affiliations'].append(idrec['supOrgName'])
        # Add managedTeams
        if 'managedTeams' in idrec:
            if 'affiliations' not in rec:
                rec['affiliations'] = []
            for mtr in idrec['managedTeams']:
                if 'supOrgName' in mtr and mtr['supOrgName'] \
                   and mtr['supOrgName'] not in rec['affiliations']:
                    rec['affiliations'].append(mtr['supOrgName'])
    if 'affiliations' in rec:
        rec['affiliations'].sort()


def get_author_counts(tag, year, show, doi_coll, orcid_coll):
    ''' Generate author counts
        Keyword arguments:
          tag: author tag
          year: publication year
          show: journal or all
          doi_coll: dois collection
          orcid_coll: orcid collection
        Returns:
          Author counts dictionary
    '''
    try:
        rows = orcid_coll.find({"affiliations": tag})
    except Exception as err:
        raise err
    author = {}
    for auth in rows:
        author[auth['employeeId']] = auth['family'][0] + ", " + auth['given'][0]
    payload = [{"$unwind": "$jrc_author"},
               {"$match": {"jrc_author": {"$in": list(author.keys())},
                           "jrc_tag.name": tag}},
               {"$group": {"_id": "$jrc_author", "count": {"$sum": 1}}}
              ]
    if year != 'All':
        payload[1]['$match']['jrc_publishing_date'] = {"$regex": "^"+ year}
    if show == 'journal':
        payload[1]['$match']["$or"] = [{"type": "journal-article"}, {"subtype": "preprint"}]
    try:
        rows = doi_coll.aggregate(payload)
    except Exception as err:
        raise err
    counts = {}
    for row in rows:
        counts[author[row['_id']]] = row['count']
    return counts


def get_author_details(rec, coll=None):
    ''' Generate a detailed author list from a DOI record
        Keyword arguments:
          data: DOI data record
          coll: optional orcid collection
        Returns:
          Detailed author list
    '''
    auth_list = []
    datacite = bool('DOI' not in rec)
    given = 'given'
    family = 'family'
    if datacite:
        given = 'givenName'
        family = 'familyName'
    field = 'creators' if datacite else 'author'
    if field not in rec and 'name' not in rec:
        return None
    author = rec[field]
    seq = 0
    for auth in author:
        payload = {}
        _set_paper_orcid(auth, datacite, payload)
        seq += 1
        if datacite and (given not in auth or not auth[given]) \
           and 'name' in auth and " " in auth['name']:
            payload['given'] = auth['name'].split(" ")[0]
            payload['family'] = auth['name'].split(" ")[-1]
        else:
            if family in auth:
                payload['family'] = auth[family].replace('\xa0', ' ')
            elif 'name' in auth:
                payload['name'] = auth['name']
            if given in auth:
                payload['given'] = auth[given].replace('\xa0', ' ')
            else:
                payload['given'] = ''
        if seq == 1 or ('sequence' in auth and auth['sequence'] == 'first'):
            payload['is_first'] = True
        if seq == len(author):
            payload['is_last'] = True
        if 'ORCID' in auth:
            payload['orcid'] = auth['ORCID'].split("/")[-1]
        affiliations = []
        if 'affiliation' in auth and auth['affiliation']:
            for aff in auth['affiliation']:
                if datacite:
                    affiliations.append(aff)
                else:
                    if 'name' in aff and aff['name']:
                        affiliations.append(aff['name'])
        if affiliations:
            payload['affiliations'] = affiliations
        LLOGGER.debug(f"Payload: {payload}")
        if coll is not None:
            try:
                _add_single_author_jrc(payload, coll)
            except Exception as err:
                raise err
        auth_list.append(payload)
    if not auth_list:
        return None
    return auth_list


def get_author_list(rec, orcid=False, style='dis', returntype='text', project_map=None):
    ''' Generate a text author list
        Keyword arguments:
          data: data record
          orcid: generate ORCID links
          style: list style (dis or flylight)
          returntype: return type (text or list)
        Returns:
          Text author list
    '''
    auth_list = []
    datacite = bool('DOI' not in rec)
    given = 'given'
    family = 'family'
    if datacite:
        given = 'givenName'
        family = 'familyName'
    field = 'creators' if datacite else 'author'
    if field not in rec and 'editor' in rec:
        field = 'editor'
    if field not in rec:
        print(rec)
        return None
    author = rec[field]
    punc = '.' if style == 'flylight' else ''
    for auth in author:
        full = ""
        if (project_map is not None) and given in auth and family in auth:
            full = f"{auth[given]} {auth[family]}"
            try:
                row = project_map.find_one({"name": full})
            except Exception as err:
                raise err
            if not row:
                full = ""
        if not full:
            if given in auth:
                initials = auth[given].split()
                first = []
                for gvn in initials:
                    first.append(gvn[0] + punc)
                if style == 'flylight':
                    full = ', '.join([auth[family], ' '.join(first)])
                else:
                    full = ', '.join([auth[family], ''.join(first)])
            elif family in auth:
                full = auth[family]
            elif 'name' in auth:
                full = auth['name']
            else:
                continue
        if 'ORCID' in auth and orcid:
            full = f"<a href='{auth['ORCID']}' target='_blank'>{full}" \
                   + "<img alt='ORCID logo' " \
                   + f"src='{ORCID_LOGO}' width='16' height='16' /></a>"
        auth_list.append(full)
    if not auth_list:
        return None
    if returntype == 'list':
        return auth_list
    last = auth_list.pop()
    if not auth_list:
        return last
    if last[-1] != '.':
        last += '.'
    punc = ' & ' if style == 'flylight' else ', '
    if auth_list:
        return  ', '.join(auth_list) + punc + last
    return None


def get_doi_record(doi, coll):
    ''' Return a record from the dois collection
        Keyword arguments:
          doi:: DOI
          coll: dois collection
        Returns:
          None
    '''
    try:
        row = coll.find_one({"doi": doi})
    except Exception as err:
        raise err
    return row


def get_dois_by_author(author, coll):
    ''' Get DOIs by author
        Keyword arguments:
          author: record from ORCID collection
          coll: dois collection
        Returns:
          List of DOIs
    '''
    payload = {"$or": [{"author.family": {"$in": author['family']},
                        "author.given": {"$in": author['given']}},
                       {"creators.familyName": {"$in": author['family']},
                        "creators.givenName": {"$in": author['given']}}
                      ]}
    try:
        rows = coll.find(payload, {"doi": 1})
    except Exception as err:
        raise err
    return [row['doi'] for row in rows]


def get_first_last_author_payload(doi):
    ''' Get the first and last author payload for a DOI
        Keyword arguments:
          doi: DOI
        Returns:
          First and last author payload
    '''
    try:
        headers = {"Authorization": f"Bearer {os.environ['DIS_JWT']}"}
        authors = requests.get(f"{DIS_URL}doi/authors/{doi}",
                               headers=headers, timeout=10).json()
    except Exception as err:
        raise err
    first = []
    first_id = []
    last = None
    last_id = None
    for auth in authors['data']:
        if not auth['in_database']:
            continue
        name = ", ".join([auth['family'], auth['given']])
        if 'is_first' in auth and auth['is_first']:
            first.append(name)
            if 'employeeId' in auth and auth['employeeId']:
                first_id.append(auth['employeeId'])
        if 'is_last' in auth and auth['is_last']:
            last = name
            if 'employeeId' in auth and auth['employeeId']:
                last_id = auth['employeeId']
    pset = {}
    if first:
        pset['jrc_first_author'] = first
        if first_id:
            pset['jrc_first_id'] = first_id
    if last:
        pset['jrc_last_author'] = last
        if last_id:
            pset['jrc_last_id'] = last_id
    if pset:
        payload = {"$set": pset}
    else:
        payload = {"$unset": {'jrc_first_author': None, 'jrc_first_id':  None,
               'jrc_last_author': None, 'jrc_last_id': None}}
    LLOGGER.debug(f"First/last payload: {payload}")
    return payload


def get_journal(rec, full=True):
    ''' Generate a journal name
        Keyword arguments:
          rec: data record
          full: adds journal and page
        Returns:
          Journal name
    '''
    if 'DOI' in rec:
        # Crossref
        if 'short-container-title' in rec and rec['short-container-title']:
            journal = rec['short-container-title'][0]
        elif 'container-title' in rec and rec['container-title']:
            journal = rec['container-title'][0]
        elif 'institution' in rec:
            if isinstance(rec['institution'], list):
                journal = rec['institution'][0]['name']
            else:
                journal = rec['institution']['name']
        elif 'elife' in rec['DOI'] and 'subtype' in rec and rec['subtype'] == 'preprint':
            journal = 'eLife'
        elif 'osf.io' in rec['DOI']:
            journal = 'osf.io'
        elif 'peerj.preprints' in rec['DOI']:
            journal = 'PeerJ'
        elif 'protocols.io' in rec['DOI']:
            journal = 'protocols.io'
        else:
            return None
        year = get_publishing_date(rec)
        if year == 'unknown':
            return None
        journal += '. ' + year.split('-')[0]
        if full:
            if 'volume' in rec:
                journal += '; ' + rec['volume']
            if 'page' in rec:
                journal += ': ' + rec['page']
            else:
                journal += ': ' + rec['DOI'].split('/')[-1]
        return journal
    # DataCite
    year = get_publishing_date(rec)
    if year == 'unknown':
        return None
    if 'publisher' in rec and rec['publisher']:
        return f"{rec['publisher']}. {year.split('-')[0]}"
    return None


def get_name_combinations(idrec, rec):
    ''' Add name combinations
        Keyword arguments:
          idrec: record from HHMI's People service
          rec: orcid collection record
        Returns:
          None
    '''
    if idrec:
        for source in ('nameFirst', 'nameFirstPreferred'):
            if source in idrec and idrec[source] and idrec[source] not in rec['given']:
                rec['given'].append(idrec[source])
        for source in ('nameLast', 'nameLastPreferred'):
            if source in idrec and idrec[source] and idrec[source] not in rec['family']:
                rec['family'].append(idrec[source])
        for source in ('nameMiddle', 'nameMiddlePreferred'):
            if source not in idrec or not idrec[source]:
                continue
            for first in rec['given']:
                if ' ' in first:
                    continue
                new = f"{first} {idrec[source][0]}"
                if new not in rec['given']:
                    rec['given'].append(new)
                new += '.'
                if new not in rec['given']:
                    rec['given'].append(new)
    _process_middle_initials(rec)


def get_project_map(coll):
    ''' Get projects from the project_map collection
        Keyword arguments:
          coll: project_map collection
        Returns:
          Project mapping dict
    '''
    project = {}
    try:
        rows = coll.find({})
    except Exception as err:
        raise err
    for row in rows:
        project[row['name']] = row['project']
    return project


def get_projects_from_dois(dcoll, ocoll=None):
    ''' Get projects from the name field in the dois collection. Note that the count may not
        be accurate due to DataCite inconsistencies.
        Keyword arguments:
          dcoll: dois collection
          ocoll: orcid collection
        Returns:
          Project dict
    '''
    payload =[{"$match": {"author.name": {"$exists": True}}},
              {"$unwind": "$author"},
              {"$match": {"author.name": {"$exists": True},
                          "author.familyName": {"$exists": False}}},
              {"$group": {"_id": "$author.name", "count": {"$sum": 1}}}]
    try:
        rows = dcoll.aggregate(payload)
    except Exception as err:
        raise err
    proj = {}
    for row in rows:
        proj[row['_id']] = row['count']
    payload = [{"$match": {"creators.name": {"$exists": True}}},
               {"$unwind": "$creators"},
               {"$match": {"creators.name": {"$exists": True},
                           "creators.familyName": {"$exists": False}}},
               {"$group": {"_id": "$creators.name", "count": {"$sum": 1}}}]
    try:
        rows = dcoll.aggregate(payload)
    except Exception as err:
        raise err
    for row in rows:
        if row['_id'] in proj:
            proj[row['_id']] += row['count']
        else:
            proj[row['_id']] = row['count']
    if ocoll is None:
        return proj
    for key in sorted(proj.keys()):
        try:
            cnt = ocoll.count_documents({"$or": [{"family": key.split(' ')[-1]},
                                                 {"given": key.split(' ')[-1]}]})
        except Exception as err:
            raise err
        if cnt:
            del proj[key]
    return proj


def get_publishing_date(rec):
    """ Return the publication date
        published:
        published-print:
        published-online:
        posted:
        created:
        Keyword arguments:
          rec: Crossref or DataCite record
        Returns:
          Publication date
    """
    if 'DOI' in rec:
        # Crossref
        for sec in ('published', 'published-print', 'published-online', 'posted', 'created'):
            if sec in rec and 'date-parts' in rec[sec] and len(rec[sec]['date-parts'][0]) == 3:
                arr = rec[sec]['date-parts'][0]
                try:
                    return '-'.join([str(arr[0]), f"{arr[1]:02}", f"{arr[2]:02}"])
                except Exception as err:
                    raise err
    else:
        # DataCite
        if 'registered' in rec:
            return rec['registered'].split('T')[0]
    return 'unknown'


def get_single_author_details(rec, coll=None):
    ''' Generate a detail dict for a single author from the orcid collection
        Keyword arguments:
          rec: orcid data record
          coll: optional orcid collection
        Returns:
          Detailed author list
    '''
    payload = rec
    if coll is not None:
        try:
            _add_single_author_jrc(rec, coll)
        except Exception as err:
            raise err
        payload['asserted'] = False
        return payload
    return None



def get_supervisory_orgs(coll=None):
    ''' Get supervisory organizations from HQ (default) or MongoDB
        Keyword arguments:
          coll: suporg collection object (optional)
        Returns:
          Dict of supervisory organizations
    '''
    orgs = {}
    if coll is not None:
        try:
            rows = coll.find({})
        except Exception as err:
            raise err
        for row in rows:
            orgs[row['name']] = {'code': row['code']}
            if 'active' in row:
                orgs[row['name']]['active'] = True
        return orgs
    try:
        resp = requests.get(ORGS_URL, timeout=10)
        results = resp.json()['result']
    except Exception as err:
        raise err
    if resp.status_code != 200:
        raise Exception(f"Failed to get supervisory organizations: {resp.status_code}")
    for org in results:
        if org['LOCATIONCODE'] and 'Janelia' in org['LOCATIONCODE'] and 'SUPORGCODE' in org:
            orgs[org['SUPORGNAME']] = org['SUPORGCODE']
    return orgs


def get_title(rec):
    ''' Generate a title
        Keyword arguments:
          data: data record
        Returns:
          Title
    '''
    if 'DOI' in rec:
        # Crossref
        if 'title' in rec and rec['title'] and rec['title'][0]:
            return rec['title'][0]
        return 'No title'
    # DataCite
    if 'titles' in rec and rec['titles'] and 'title' in rec['titles'][0]:
        return rec['titles'][0]['title']
    return 'No title'


def is_datacite(doi):
    ''' Determine if a DOI is from DataCite or not.
        Keyword arguments:
          doi: DOI
        Returns:
          True or False
    '''
    doilc = doi.lower()
    return bool("/janelia" in doilc or "/arxiv" in doilc or "/d1." in doilc \
                or "/dryad" in doilc or "/micropub.biology" in doilc \
                or "/zenodo" in doilc or "figshare" in doilc)


def is_janelia_author(auth, coll, project):
    ''' Determine if an author is a Janelian or not
        Keyword arguments:
          auth: single author record from Crossref or DataCite
          coll: orcid collection
          project: project mapping dict
        Returns:
          Author name (if true) or None
    '''
    # Determine source
    if 'familyName' in auth or 'nameIdentifiers' in auth:
        datacite = True
        family = 'familyName'
        given = 'givenName'
    else:
        datacite = False
        family = 'family'
        given = 'given'
    # Affiliation
    if 'affiliation' in auth:
        for aff in auth['affiliation']:
            if datacite:
                if "Janelia" in aff:
                    return auth['name']
                continue
            if 'name' in aff:
                if "Janelia" in aff['name']:
                    return " ".join([auth[given], auth[family]])
            if 'id' in aff:
                # If there's no name in the record, see if we can process an ROR ID
                for aid in aff['id']:
                    if 'id-type' in aid and aid['id-type'] == 'ROR' \
                       and 'id' in aid and aid['id'].endswith(f"/{JANELIA_ROR}"):
                        return " ".join([auth[given], auth[family]])
    # Project name
    if datacite:
        if family in auth and 'name' in auth and auth['name'] == auth[family]:
            if auth['name'] in project:
                return project[auth['name']]
            return None
        if family not in auth and given not in auth and 'name' in auth:
            if auth['name'] in project:
                return project[auth['name']]
            return None
    elif 'name' in auth:
        if auth['name'] in project:
            return project[auth['name']]
        LLOGGER.error(auth)
        raise Exception(f"Unknown Crossref project: {auth['name']}")
    # Name
    try:
        payload = {"family": auth[family], "given": auth[given]}
    except Exception as err:
        raise Exception(f"Missing name: {auth}") from err
    if "ORCID" in auth:
        payload['orcid'] = auth['ORCID'].split("/")[-1]
    try:
        _add_single_author_jrc(payload, coll)
    except Exception as err:
        LLOGGER.error("Failed _add_single_author_jrc")
        raise err
    if 'in_database' in payload and payload['in_database'] and not payload['alumni']:
        return " ".join([auth[given], auth[family]])
    return None


def is_journal(rec):
    ''' Determine if a resource is a journal article or not
        Keyword arguments:
          rec: DOI record
        Returns:
          True or False
    '''
    # Crossref
    if ('type' in rec) and (rec['type'] == 'journal-article') \
       and (('subtype' not in rec) or (not rec['subtype'])):
        return True
    # DataCite
    if ('types' in rec) and ('resourceTypeGeneral' in rec['types']) \
       and (rec['types']['resourceTypeGeneral'] == 'DataPaper'):
        return True
    return False


def is_preprint(rec):
    ''' Determine if a resource is a preprint or not
        Keyword arguments:
          rec: DOI record
        Returns:
          True or False
    '''
    # Crossref
    if ('subtype' in rec) and (rec['subtype'] == 'preprint'):
        return True
    # DataCite
    if ('types' in rec) and ('resourceTypeGeneral' in rec['types']) \
       and (rec['types']['resourceTypeGeneral'] == 'Preprint'):
        return True
    return False


def short_citation(doi, expanded=False):
    ''' Generate a short citation
        Keyword arguments:
          doi: DOI
          expanded: add title, journal, and PMID
        Returns:
          Short citation
    '''
    try:
        if is_datacite(doi):
            rec = JRC.call_datacite(doi)
            if rec is None or 'data' not in rec:
                return None
            rec = rec['data']['attributes']
            authors = rec['creators']
        else:
            rec = JRC.call_crossref(doi)
            if rec is None or 'message' not in rec:
                return None
            rec = rec['message']
            if 'author' in rec:
                authors = rec['author']
            else:
                authors = rec['editor']
    except Exception as err:
        raise err
    pdate = " " + get_publishing_date(rec).split('-')[0]
    pmid = JRC.get_pmid(doi)
    if pmid and 'status' in pmid and pmid['status'] == 'ok' \
               and 'pmid' in pmid['records'][0]:
        pmid = pmid['records'][0]['pmid']
        pmid = f" <a href='https://pubmed.ncbi.nlm.nih.gov/{pmid}'>{pmid}</a>"
    else:
        pmid = ""
    jour = ""
    if expanded:
        jour = get_journal(rec, False)
        if jour:
            jour = f" {jour}."
            pdate = ""
        else:
            jour = ""
        ttl = get_title(rec)
        if ttl:
            jour = f" {ttl}.{jour}"
    if is_datacite(doi):
        if 'familyName' not in authors[0]:
            if 'name' in authors[0]:
                authors[0]['familyName'] = authors[0]['name']
            else:
                authors[0]['familyName'] = 'Unknown author'
        if len(authors) > 1:
            return f"{authors[0]['familyName']} et al.{jour}{pdate}{pmid}"
        return f"{authors[0]['familyName']}.{jour}{pdate}{pmid}"
    rec['DOI'] = doi
    for auth in authors:
        if 'family' not in auth or auth['sequence'] != 'first':
            break
        if len(authors) > 1:
            return f"{authors[0]['family']} et al.{jour}{pdate}{pmid}"
        return f"{authors[0]['family']}.{jour}{pdate}{pmid}"
    return None


def single_orcid_lookup(val, coll, lookup_by='orcid'):
    ''' Lookup a single row in the orcid collection
        Keyword arguments:
          val: ORCID or employeeId
          coll: orcid collection
          lookup_by: "orcid" or "employeeId"
        Returns:
          row from collection
    '''
    if lookup_by not in ('orcid', 'employeeId'):
        raise ValueError("Invalid lookup_by in single_orcid_lookup")
    try:
        row = coll.find_one({lookup_by: val})
    except Exception as err:
        raise err
    if row:
        try:
            cnt = coll.count_documents({"given": row['given'], "family": row['family']})
        except Exception as err:
            raise err
        if cnt > 1:
            row['duplicate_name'] = True
    return row


def add_doi_to_process(doi, coll, write=True):
    ''' Add a DOI to the dois_to_process collection
        Keyword arguments:
          doi: DOI
          coll: dois_to_process collection
          write: write to dois_to_process collection
        Returns:
          row ID if written, payload if not. Will return None if DOI is already in collection.
    '''
    try:
        row = coll.find_one({"doi": doi})
    except Exception as err:
        raise err
    if row:
        raise ValueError(f"DOI {doi} already in process collection")
    payload = {"doi": doi,
               "inserted": datetime.today().replace(microsecond=0)}
    if not write:
        return payload
    try:
        result = coll.insert_one(payload)
    except Exception as err:
        raise err
    return result.inserted_id


def add_orcid(eid, coll, given=None, family=None, orcid=None, write=True):
    ''' Add a record to the orcid collection
        Keyword arguments:
          eid: employeeId
          coll: orcid collection
          given: list of given names
          family: list of family names
          orcid: ORCID
          write: write to orcid collection
        Returns:
          New record
    '''
    try:
        row = coll.find_one({"employeeId": eid})
    except Exception as err:
        raise err
    if row:
        raise ValueError(f"EmployeeId {eid} already in orcid collection")
    try:
        resp = JRC.call_people_by_id(eid)
    except Exception as err:
        raise err
    if not resp:
        raise ValueError(f"EmployeeId {eid} is not in the People system")
    LLOGGER.debug(f"People record: {resp}")
    payload = {"employeeId": eid,
               "userIdO365": resp['userIdO365']
              }
    if orcid:
        try:
            oid = coll.find_one({"orcid": orcid})
        except Exception as err:
            raise err
        if oid:
            raise ValueError(f"ORCID {orcid} already in orcid collection")
        payload['orcid'] = orcid
    get_affiliations(resp, payload)
    if not given or not family:
        payload['given'] = []
        payload['family'] = []
    else:
        payload['given'] = given
        payload['family'] = family
    get_name_combinations(resp, payload)
    LLOGGER.debug(f"Payload: {payload}")
    if not write:
        return payload
    try:
        result = coll.insert_one(payload)
    except Exception as err:
        raise err
    payload["_id"] = str(result.inserted_id)
    return payload


def add_orcid_name(lookup_by='employeeId', lookup=None, family=None, given=None,
                   coll=None, write=True):
    ''' Add names to a record in the orcid collection
        Keyword arguments:
          lookup_by: "employeeId" or "orcid"
          lookup: lookup value
          family: list of family names
          given: list of given names
          coll: orcid collection
          write: write record to orcid collection [True]
        Returns:
          Updated record
    '''
    if lookup_by not in ('orcid', 'employeeId'):
        raise ValueError("Invalid lookup_by in add_orcid_name")
    try:
        row = coll.find_one({lookup_by: lookup})
    except Exception as err:
        raise err
    if not row:
        raise ValueError(f"{lookup_by} {lookup} not found in add_orcid_name")
    changes = False
    if family:
        for name in family:
            if name not in row['family']:
                row['family'].append(name)
                changes = True
    if given:
        for name in given:
            if name not in row['given']:
                row['given'].append(name)
                changes = True
    if (not changes) or (not write):
        return None
    payload = {"$set": {'family': row['family'], 'given': row['given']}}
    LLOGGER.debug(f"Payload: {payload}")
    try:
        result = coll.update_one({"_id": row['_id']}, payload)
    except Exception as err:
        raise err
    if hasattr(result, 'matched_count') and result.matched_count:
        try:
            row = coll.find_one({lookup_by: lookup})
        except Exception as err:
            raise err
        return row
    return None


def update_existing_orcid(lookup=None, add=None, coll=None,
                          lookup_by='employeeId', write=True):
    ''' Update a single row in the orcid collection with a new employeeId or ORCID
        Keyword arguments:
          lookup: lookup value
          add: data to insert/update
          lookup_by: "orcid" or "employeeId"
          coll: orcid collection
          write: write record to orcid collection [True]
        Returns:
          row from collection (or None if update was not performed)
    '''
    if lookup_by not in ('orcid', 'employeeId'):
        raise ValueError("Invalid lookup_by in update_existing_orcid")
    try:
        row = coll.find_one({lookup_by: lookup})
    except Exception as err:
        raise err
    if not row:
        raise ValueError(f"{lookup_by} {lookup} not found in update_existing_orcid")
    field = 'employeeId' if lookup_by == 'orcid' else 'orcid'
    payload = {"$set": {field: add}}
    LLOGGER.debug(f"Payload: {payload}")
    if not write:
        return None
    try:
        result = coll.update_one({"_id": row['_id']}, payload)
    except Exception as err:
        raise err
    if hasattr(result, 'matched_count') and result.matched_count:
        try:
            row = coll.find_one({lookup_by: lookup})
        except Exception as err:
            raise err
        return row
    return None


def update_jrc_fields(doi, doi_coll, payload, write=True):
    ''' Update jrc_ fields in a single DOI
        Keyword arguments:
          doi: DOI
          doi_coll: dois collection
          payload: payload to update DOI with
          write: write to dois collection
        Returns:
          None or the number of rows updated
    '''
    if (not doi) or (doi_coll is None) or (not payload):
        raise Exception("Missing arguments in update_jrc_fields")
    try:
        row = doi_coll.find_one({"doi": doi})
    except Exception as err:
        raise err
    if not row:
        raise Exception(f"DOI {doi} not found in update_jrc_fields")
    for key in payload:
        if not key.startswith("jrc_"):
            raise Exception("All fields in payload must start with 'jrc_'")
    if not write:
        return None
    payload = {"$set": payload}
    try:
        result = doi_coll.update_one({"doi": doi}, payload)
    except Exception as err:
        raise err
    return result.matched_count if hasattr(result, 'matched_count') else None


def update_jrc_author_from_doi(doi, doi_coll, orcid_coll, write=True):
    ''' Update jrc_author tag for a single DOI
        Keyword arguments:
          doi: DOI
          doi_coll: dois collection
          orcid_coll: orcid collection
          write: write to dois collection
        Returns:
          list of Janelia authors
    '''
    try:
        row = doi_coll.find_one({"doi": doi})
    except Exception as err:
        raise err
    try:
        authors = get_author_details(row, orcid_coll)
    except Exception as err:
        raise err
    jrc_author = []
    for auth in authors:
        if auth['janelian'] and 'employeeId' in auth and auth['employeeId']:
            jrc_author.append(auth['employeeId'])
    if write:
        if jrc_author:
            payload = {"$set": {"jrc_author": jrc_author}}
        else:
            payload = {"$unset": {"jrc_author": 1}}
        try:
            _ = doi_coll.update_one({"doi": doi}, payload)
        except Exception as err:
            raise err
    return jrc_author
