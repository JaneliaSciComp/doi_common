import json
import doi_common as DL

print(json.dumps(DL.get_doi_record('10.1126/science.aeb0813', source='unpaywall'), indent=2))
