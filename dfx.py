"""
Tests
"""

import argparse,csv,json,re #,ast (for literal_eval, not yet used)
from os import path
from shutil import copy
from df_fn import xmetaj,xfieldj,DFMeta,DFCol,DFOutCol,DFOutColAsIs,autosuggestor
from df_fn import makeTailUnq,qb2py,n2str,handleDelimFile

# to allow ridiculously large cells
csv.field_size_limit(100000000)

# Which tests
test_all = False
test_cmpdfms = False or test_all
test_localdb = False or test_all

# non active diags: ('DiagObs:MEDICAL_HX','PROBLEM_STATUS_C:3','PROBLEM_STATUS_C:2')

"""
Note: the following works:
xfieldj(testjson,**testargs['match_mc'])

DONE: Write the rules.py rules for active and inactive diagnoses
TODO: implement reading of explicit column configuration file
TODO: On the df.py side, create an extra concept_cd||modifier_cd field
TODO: On the df.py side, do the concept code collapsing using concept_dimension
TODO: On the df.py side, do modifier mapping 
DONE: Figure out best way to hand over fresh output file from df.py directly to dfx.py
TODO: Start chopping out the no longer needed stuff from df.py
TODO: some kind of progress indicator so we can tell that dfx.py is not hung
DONE: What if the data argument is --None-- or not JSON?
DONE: Iterate over a list of extractors for the same cell.
DONE: Have a list of lists of extractors and iterate over it for a line, returning the raw values for cells
      that are not JSON objects
DONE: Store the extractors in one dict per cell, with the dicts in a list with the same number of rows as 
      there are columns in the input data
DONE: Populate such a list from JSON strings in the first row of the input data and a set of rules for which
      default extractor goes with which set of conditions (and which extractors are invalid for which 
      conditions)
DONE: Generate the JSON strings in df.py by reading the data dictionary.
DONE: update testargs with the intended values for: as_is, concat_unique, last_numeric, last_unique
      true_false, true_false_active
DONE: actually start processing the rows!
DONE: multiple extractors for one field
DONE: create the new first row for the output, with the non-meta columns having null values 
      or something
DONE: for more robustness, try json.loads and return empty string if failed
"""

from rules import rules

if(test_localdb):
  copy('exampleinput.db','testin0.db')
  dfmdb = DFMeta('testin0.db',suggestions=autosuggestor)

copy('exampleoutput.csv','testin1.csv')
dfm = DFMeta('testin1.csv',suggestions=autosuggestor)

if(test_cmpdfms):
  # comparison of two different methods of dfmeta creation
  if path.isfile('../www/demodata.csv'):
    copy('../www/demodata.csv','testin2.csv')
    dfm2 = DFMeta('testin2.csv',suggestions=autosuggestor)
  if path.isfile('../www/demodata.db'):
    copy('../www/demodata.db','testin3.db')
    dfm3 = DFMeta('testin3.db',suggestions=autosuggestor)

  try:
    # only needed for testing, not intended to be part of this package
    from deepdiff import DeepDiff
    dfmDiff = DeepDiff(dfm2,dfm3)
    if dfmDiff: print '''
    Have a look at dfmDiff, something is different between dfm0 (csv input)
    and dfm1 (db input but identical data)'''
  except: pass

try: dfc = dfm.incols['v113_RDW_RBC_At_Rt'] #['v036_CS_Mts_at_DX']
except: dfc = dfm.incols['v006_Hrt_Rt_LNC']
colids=dfc.getColIDs(childtype='rules'
		     ,childids= ['selid','addbid','shortname','longname'
		   ,'ruledesc','parent_name'])

# creates FOO_tf
testch0 = dfc.prepChosen(dfc.rules['true_false'])
			  #,userArgs={'MM':123})
# creates FOO_lnc277e
testch1 = dfc.prepChosen(dfc.rules['last_numeric']
			  ,userArgs={'aa':'bb','CC': 123})
# creates FOO_lnc9947
testch2 = dfc.prepChosen(dfc.rules['last_numeric']
			      ,userArgs={'vv':'bb','CC': 123, 'qq': 42})
# updates FOO_tf despite different args
testch3 = dfc.prepChosen(dfc.rules['true_false'],userArgs={'aa':'bb','CC': 123})
# updates FOO_lnc9947
testch4 = dfc.prepChosen(dfc.rules['last_numeric']
			      ,userArgs={'vv':'bb','CC': 123, 'qq': 42})
# creates FOO_lnc9f70
testch5 = dfc.prepChosen(dfc.rules['last_numeric']
			      ,userArgs={'aa':'bb','CC': 124})
# creates FOO_lncb208
testch6 = dfc.prepChosen(dfc.rules['last_numeric']
			      ,userArgs={'aa':'bb','CC': 124, 'qq': 42})
# error
#testch7 = dfc.prepChosen(dfc.rules['last_numeric_fltrcode'])
#testOC = DFOutCol(dfc,testch0)
#testcell = json.loads(testjson)
#testdfo = testOC.processCell(testcell,testjson)
#testsugg = [xx for xx in dfc.rules.values() if xx.get('suggested')]
#testfvr = dfc.valfixRule(testsugg[0],None,['longname','selector','fieldlist','aggregator'])
#dfc.finalizeChosen()
#dfm['age_at_visit_days'].finalizeChosen()
#testUDROut = dfm.userDesignedRule(testUserRule
				  #,'custom'
				  #,['v121_mlgnt_nplsm','v011_unspcfd_mlgnt'])
try: testDelimZ = handleDelimFile('df.py')
except Exception,ee: print ee
testDelimX = handleDelimFile('threeFOO.csv')
# test a representative row of data
dfm.fhandle.seek(88271,0)
#testrow = dfm.data.next()
testfout = csv.writer(open('testfout.csv','w'),dialect=dfm.data.dialect)
testfout.writerow(dfm.getHeaders())
testfout.writerow(dfm.getMetas())
dfm.fhandle.seek(dfm.ofsdata)
while dfm.nrows <= 2003: testfout.writerow(dfm.processRow(dfm.data.next()))

# 190124: why doesn't the median aggregator work?
from testinput import c_median_00_dd
med00_rule = dfc.prepChosen(dfc.rules['med_numeric'])
med00_dfo = DFOutCol(dfc,med00_rule)
med00_proc = med00_dfo.processCell(c_median_00_dd,'')

# 190124: added hasNV filter to prevent missing values from screwing up
#         medians
from testinput import c_median_01_dd
med01_proc = med00_dfo.processCell(c_median_01_dd,'')

# 190124: testing DFColStatic
from df_fn import DFColStatic
dfcs = DFColStatic(dfm.inmeta[3],dfm.inhead[3])
import pdb; pdb.set_trace()
