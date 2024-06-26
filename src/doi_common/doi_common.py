''' doi_common.py
    Library of routines for parsing and interpreting DOI/ORCID records.
    Callable functions:
      get_affiliations
      get_author_details
      get_author_list
      get_journal
      get_name_combinations
      get_publishing_date
      get_supervisory_orgs
      get_title
      is_datacite
      single_orcid_lookup
'''

# pylint: disable=broad-exception-raised

import re
import requests

ORCID_LOGO = "https://info.orcid.org/wp-content/uploads/2019/11/orcid_16x16.png"
ORGS_URL = "https://services.hhmi.org/IT/WD-hcm/supervisoryorgs"


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
        if payload['janelian']:
            if 'group' in row:
                payload['group'] = row['group']
            if 'group_code' in row:
                payload['group_code'] = row['group_code']
            if 'affiliations' in row:
                payload['tags'] = row['affiliations']
        if 'employeeId' in row and row['employeeId']:
            payload['employeeId'] = row['employeeId']


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
    if 'orcid' in payload:
        try:
            row = coll.find_one({"orcid": payload['orcid']})
        except Exception as err:
            raise err
        _adjust_payload(payload, row)
    elif 'family' in payload:
        try:
            row = coll.find_one({"given": payload['given'], "family": payload['family']})
        except Exception as err:
            raise err
        _adjust_payload(payload, row)
    if 'affiliations' in payload and payload['affiliations']:
        for aff in payload['affiliations']:
            if 'Janelia' in aff:
                payload['janelian'] = True
                payload['asserted'] = True
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

# ******************************************************************************
# * Callable functions                                                         *
# ******************************************************************************

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



def get_author_details(rec, coll=None):
    ''' Generate a detailed author list
        Keyword arguments:
          data: data record
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
    for auth in author:
        payload = {}
        if family in auth:
            payload['family'] = auth[family].replace('\xa0', ' ')
        elif 'name' in auth:
            payload['name'] = auth['name']
        if given in auth:
            payload['given'] = auth[given].replace('\xa0', ' ')
        else:
            payload['given'] = ''
        if 'ORCID' in auth:
            payload['orcid'] = auth['ORCID'].split("/")[-1]
        affiliations = []
        if 'affiliation' in auth and auth['affiliation']:
            for aff in auth['affiliation']:
                if 'name' in aff and aff['name']:
                    affiliations.append(aff['name'])
        if affiliations:
            payload['affiliations'] = affiliations
        if coll is not None:
            try:
                _add_single_author_jrc(payload, coll)
            except Exception as err:
                raise err
        auth_list.append(payload)
    if not auth_list:
        return None
    return auth_list


def get_author_list(rec, orcid=False, style='dis', returntype='text'):
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
    if field not in rec:
        print(rec)
        return None
    author = rec[field]
    punc = '.' if style == 'flylight' else ''
    for auth in author:
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


def get_journal(rec):
    ''' Generate a journal name
        Keyword arguments:
          data: data record
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
        else:
            return "(No journal found)"
        year = get_publishing_date(rec)
        if year == 'unknown':
            return None
        journal += '. ' + year.split('-')[0]
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
        return f"{rec['publisher']}, {year.split('-')[0]}"
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


def get_supervisory_orgs():
    ''' Get supervisory organizations
        Keyword arguments:
          None
        Returns:
          Dict of supervisory organizations
    '''
    orgs = {}
    try:
        resp = requests.get(ORGS_URL, timeout=10)
        results = resp.json()['result']
    except Exception as err:
        raise err
    if resp.status_code != 200:
        raise Exception(f"Failed to get supervisory organizations: {resp.status_code}")
    for org in results:
        if 'Janelia' in org['LOCATIONCODE'] and 'SUPORGCODE' in org:
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
        if 'title' in rec and rec['title'][0]:
            return rec['title'][0]
        return None
    # DataCite
    if 'titles' in rec and rec['titles'] and 'title' in rec['titles'][0]:
        return rec['titles'][0]['title']
    return None


def is_datacite(doi):
    ''' Determine if a DOI is from DataCite or not.
        Keyword arguments:
          doi: DOI
        Returns:
          True or False
    '''
    doilc = doi.lower()
    return bool("/janelia" in doilc or "/arxiv" in doilc or "/d1." in doilc \
                or "/micropub.biology" in doilc or "/zenodo" in doilc)


def single_orcid_lookup(val, coll, lookup_by='orcid'):
    ''' Lookup a single row in the orcid collection
        Keyword arguments:
          val: ORCID or employeeId
          coll: orcid collection
          lookup_by: "orcid" or "employeeId"
        Returns:
          True or False
    '''
    if lookup_by not in ('orcid', 'employeeId'):
        raise ValueError("Invalid lookup_by")
    try:
        row = coll.find_one({lookup_by: val})
    except Exception as err:
        raise err
    return row
