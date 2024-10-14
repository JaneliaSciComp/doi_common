from operator import attrgetter
import os
import jrc_common.jrc_common as JRC
from doi_common.doi_common import get_project_map

os.environ['CONFIG_SERVER_URL'] = 'https://config.int.janelia.org/'
DB = {}
dbconfig = JRC.get_config("databases")
dbo = attrgetter("dis.prod.read")(dbconfig)
DB['dis'] = JRC.connect_database(dbo)
COLL_PM = DB['dis'].project_map

def test_get_project_map():
    pmap = get_project_map(COLL_PM)
    assert 'COSEM Project Team' in pmap