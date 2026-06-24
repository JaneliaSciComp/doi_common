from operator import attrgetter
import jrc_common.jrc_common as JRC
from doi_common.doi_common import get_supervisory_orgs

DB = {}
dbconfig = JRC.get_config("databases")
dbo = attrgetter("dis.prod.read")(dbconfig)
DB['dis'] = JRC.connect_database(dbo)
COLL_SUPORG = DB['dis'].suporg


def test_get_supervisory_orgs():
    orgs = get_supervisory_orgs()
    assert isinstance(orgs, dict)
    assert 'Campus Life' in orgs.keys()
    orgs = get_supervisory_orgs(COLL_SUPORG)
    assert isinstance(orgs, dict)
    assert 'Campus Life' in orgs.keys()