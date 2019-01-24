import json

#section cell values
'''Naming conventions...
X_SSS_NN_ZZ such that:
  X is 'j' for 'raw json' or 'c' for 'cell contents'
  SSS is rule suffix to test on this or a descriptive nickname
  NN is a sequence number of this X_SSS combo
  ZZ is where this is from
'''

j_example_00_kc='''{"0": {"ix": 57016613993402840, "vf": null, "mc": "DX|PROF:NONPRIMARY", "cc": "GENERIC_KUH_DX_ID_2449", "cf": null, "lc": null, "st": "2012-07-11", "un": null, "vt": null, "tc": null, "nv": 456, "qt": null}, "1": {"ix": 9697820316663506, "vf": null, "mc": "DiagObs:PAT_ENC_DX", "cc": "GENERIC_KUH_DX_ID_78949", "cf": null, "lc": null, "st": "2012-07-11", "un": null, "vt": null, "tc": null, "nv": null, "qt": null}, "2": {"ix": 55916360278195536, "vf": null, "mc": "DiagObs:PAT_ENC_DX", "cc": "GENERIC_KUH_DX_ID_78949", "cf": null, "lc": null, "st": "2012-07-11", "un": null, "vt": null, "tc": null, "nv": 123, "qt": null}, "count": 3}'''
c_example_00_kc=json.loads(j_example_00_kc)

'''
190124: why doesn't the median aggregator work?
'''
j_median_00_dd='''{"0": {"ix": 1, "vf": null, "mc": null, "cc": "8310-5", "cf": null, "lc": null, "st": "2004-09-01", "un": "F", "vt": null, "tc": null, "nv": 97, "qt": null}, "count": 1}'''
c_median_00_dd=json.loads(j_median_00_dd)

'''
190124: why are nulls screwing up medians?
'''
j_median_01_dd='''{"0": {"ix": 1, "vf": null, "mc": null, "cc": "GENERIC_NDC_00005306343", "cf": null, "lc": null, "st": "2009-03-09", "un": null, "vt": null, "tc": null, "nv": null, "qt": null}, "1": {"ix": 1, "vf": null, "mc": "MED:DOSE", "cc": "GENERIC_NDC_00005306343", "cf": null, "lc": null, "st": "2009-03-09", "un": "mg", "vt": null, "tc": null, "nv": 4, "qt": null}, "2": {"ix": 1, "vf": null, "mc": "MED:FREQ", "cc": "GENERIC_NDC_00005306343", "cf": null, "lc": null, "st": "2009-03-09", "un": null, "vt": "T", "tc": "QD", "nv": null, "qt": null}, "3": {"ix": 1, "vf": null, "mc": "MED:ROUTE", "cc": "GENERIC_NDC_00005306343", "cf": null, "lc": null, "st": "2009-03-09", "un": null, "vt": "T", "tc": "PO", "nv": null, "qt": null}, "count": 4}'''
c_median_01_dd=json.loads(j_median_01_dd)

#end_section cell values



#section prefab rules
'''Naming conventions...
X_SSS_NN_ZZ such that:
  X is 'r' for 'rule'
  SSS is the rulesuffix if available descriptive nickname otherwise
  NN is a sequence number of this X_SSS combo
  ZZ is where this is from
'''
r_lnc_00_kc={'suggested': False, 'parent_name': u'v113_RDW_RBC_At_Rt', 'selid': 'sl-v113_lnc', 'delbid': u'db-v113_lnce714', 'aggregator': 'last', 'args': 'CC', 'addbid': 'ab-v113_lnc', 'split_by_code': True, 'userArgs': {'CC': 'KUH|COMPONENT_ID:5629'}, 'selector': 'codeIn_CC', 'rulesuffix': 'lnc', 'fieldlist': 'nv', 'argsuffix': 'e714', 'criteria': 'nval_num > 0 and ccd > 1', 'rulename': 'last_numeric_fltrcode', 'shortname': u'v113_lnce714', 'longname': u'v113_RDW_RBC_At_Rt_lnce714', 'ruledesc': 'Last numeric value of the specified code for each visit.', 'divIDchosen': u'c-v113'}

#end_section prefab rules
