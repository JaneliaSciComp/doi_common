''' doi_lib.py
    Library of routines for parsing and interpreting DOI records.
    Callable functions:
      get_author_details
      get_author_list
      get_journal
      get_publishing_date
      get_title
      is_datacite
'''

ORCID_LOGO = "https://info.orcid.org/wp-content/uploads/2019/11/orcid_16x16.png"


def _add_single_author_jrc(payload, coll):
    ''' Update groups and affiliations in author detail
        Keyword arguments:
          payload: data record from author detail
          coll: orcid collection
        Returns:
          None
    '''
    if 'orcid' in payload:
        try:
            row = coll.find_one({"orcid": payload['orcid']})
        except Exception as err:
            raise err
        if row and 'alumni' not in row:
            if 'group' in row:
                payload['group'] = row['group']
            if 'affiliations' in row:
                payload['tags'] = row['affiliations']
    else:
        try:
            row = coll.find_one({"given": payload['given'], "family": payload['family']})
        except Exception as err:
            raise err
        if row and 'alumni' not in row:
            if 'group' in row:
                payload['group'] = row['group']
            if 'affiliations' in row:
                payload['tags'] = row['affiliations']


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
    if field not in rec:
        return None
    author = rec[field]
    for auth in author:
        payload = {}
        if family in auth:
            payload['family'] = auth[family].replace('\xa0', ' ')
        else:
            continue
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
