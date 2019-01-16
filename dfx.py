""" Unpack and transform variables from a .CSV file generated by df.py
---------------------------------------------------------------------
usage: dfx.py [-l] [-h] [-o OUTFILE] [csvin]
    
"""


import argparse,csv,json,re #,ast (for literal_eval, not yet used)
from os import path
from df_fn import xmetaj,xfieldj,DFMeta,DFCol,DFOutCol,DFOutColAsIs,autosuggestor
from df_fn import makeTailUnq,qb2py,n2str,handleDelimFile
# import pandas as pd (for DataFrame, not yet used)
parser = argparse.ArgumentParser()
parser.add_argument("csvin",help="CSV input file generated by df.py")
parser.add_argument("-o","--outfile",help="CSV output file",default="")
parser.add_argument("-l","--log",help="Log verbose sql",action="store_true")
args = parser.parse_args()
dolog = args.log

# to allow ridiculously large cells
csv.field_size_limit(100000000)

testjson = """{"0": {"ix": 57016613993402840, "vf": null, "mc": "DX|PROF:NONPRIMARY", "cc": "GENERIC_KUH_DX_ID_2449", "cf": null, "lc": null, "st": "2012-07-11", "un": null, "vt": null, "tc": null, "nv": 456, "qt": null}, "1": {"ix": 9697820316663506, "vf": null, "mc": "DiagObs:PAT_ENC_DX", "cc": "GENERIC_KUH_DX_ID_78949", "cf": null, "lc": null, "st": "2012-07-11", "un": null, "vt": null, "tc": null, "nv": null, "qt": null}, "2": {"ix": 55916360278195536, "vf": null, "mc": "DiagObs:PAT_ENC_DX", "cc": "GENERIC_KUH_DX_ID_78949", "cf": null, "lc": null, "st": "2012-07-11", "un": null, "vt": null, "tc": null, "nv": 123, "qt": null}, "count": 3}"""

testargs = {
  'as_is' : {'field':'','as_is' : True}
  ,'concat_unique' : {'field':'cc'}
  ,'last_numeric':{'field':'nv','transform':lambda xx: '' if len(xx) == 0 else xx.pop()}
  ,'last_unique':{'field':'cc','transform':lambda xx: '' if len(xx) == 0 else xx.pop()}
  ,'true_false':{'field':'cc','transform':any}
  ,'tf_activediag':{'field':'cc','transform':any
		    ,'select':lambda xx: [kk not in ('DiagObs:MEDICAL_HX','PROBLEM_STATUS_C:3','PROBLEM_STATUS_C:2') for kk in xx]
		    ,'nulls_r_false': True}
  ,'tf_inactivediag':{'field':'cc','transform':any
		      ,'select':lambda xx: [kk in ('DiagObs:MEDICAL_HX','PROBLEM_STATUS_C:3','PROBLEM_STATUS_C:2') for kk in xx]
		      ,'nulls_r_false': True}
  #,'':{'field':'','transform':None}
  #,'num_ix': {'field':'ix','transform':len}
  #,'any_vf': {'field':'vf','transform':any}
  #,'encdx_mc': {'field':'mc','transform':lambda xx,refval: any([kk == refval for kk in xx]),'refval':'DiagObs:PAT_ENC_DX'}
  #,'npdx_mc': {'field':'mc','transform':lambda xx,refval: any([kk == refval for kk in xx]),'refval':'DX|PROF:NONPRIMARY'}
  #,'max_st':{'field':'st','transform':max}
  #,'min_st':{'field':'st','transform':min}
  #,'first_un':{'field':'un','transform':lambda xx: [kk for kk in xx if kk is not None][0] if any(xx) else None}
  #,'':{'field':'','transform':None}
  ,}

# non active diags: ('DiagObs:MEDICAL_HX','PROBLEM_STATUS_C:3','PROBLEM_STATUS_C:2')

"""
Note: the following works:

xfieldj(testjson,**testargs['match_mc'])

TODO: Write the rules.py rules for active and inactive diagnoses
TODO: implement reading of explicit column configuration file
TODO: On the df.py side, create an extra concept_cd||modifier_cd field
TODO: On the df.py side, do the concept code collapsing using concept_dimension
TODO: On the df.py side, do modifier mapping 
TODO: Figure out best way to hand over fresh output file from df.py directly to dfx.py
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


def update_df(csvin):
  # get arguments needed to create output file
  mydir = path.dirname(csvin)
  outsuffix = '.out'
  filebase,filext = path.splitext(path.basename(csvin))
  fileout = path.join(mydir,(filebase+outsuffix+filext))

  # read the csvin file
  inhandle = open(csvin,'r'); outhandle = open(fileout,'w')
  fr = csv.reader(inhandle); fw = csv.writer(outhandle)

  # first row: header
  myheader = fr.next()

  # second row: metadata (in string form)
  rawmeta = fr.next()

  # number of columns in the input data
  rawncols = len(rawmeta)
  
  dfmeta = DFMeta(myheader,rawmeta,suggestions=autosuggestor)
  jsonout = path.join(mydir,(filebase+'_chosen.json'))
  testchoices = dfmeta.getHeaders(bycol=True)
  testchoices = {kk: [uu for uu in vv if uu['extr']!= 'as_is'] 
		 for kk,vv in testchoices.items() if len(vv) > 1}
  with open(jsonout,'w') as outfile:
    json.dump(testchoices,outfile,indent=1,sort_keys=True)
  #import pdb; pdb.set_trace()
  # experimental tree-like template and iterating over it
  #template2 = {kk: {
    #'dat': vv,'outcols':[{'cname':kk,'extr':'as_is'
			  #,'dat':json.dumps(vv) if isinstance(vv,dict) else vv
			  #,'args':[]}]
    #} for kk,vv in zip(myheader,[json.loads(jj) 
				 #if re.match('\{.*\}$',str(jj)) 
				 #else jj for jj in rawmeta])};
  #newhead2 = []; newmeta2 = [];
  #[[newhead2.append(yy) for yy in 
    #[xx.get('cname') for xx in 
     #template2.get(ii).get('outcols')]] for ii in myheader];
    
  #[[newmeta2.append(yy) for yy in 
    #[xx.get('dat') for xx in 
     #template2.get(ii).get('outcols')]] for ii in myheader];
  '''
  #order gets preserved:
  all([json.loads(aa)==json.loads(bb) if re.match('\{.*\}$',aa) else aa==bb for aa,bb in zip(rawmeta,newmeta2)]);
  all([aa==bb for aa,bb in zip(myheader,newhead2)]);
  '''
  # unpack the column metadata in from the second row
  """ `template` is a list of lists (equal in length to number of input columns?)
  With each list consisting of the extractor(s?) to use, the literal header(s?)
  and the value to put in the output row 2
  """ 
  template = [xmetaj(xx[0],xx[1]) for xx in zip(rawmeta,myheader)]
  ncols = len(template)

  newhead = [] # the actual header row to write to the output file
  [[newhead.append(jj[1]) for jj in ii] for ii in template]

  newmeta = [] # the actual second row to write to the output file
  [[newmeta.append(jj[2]) for jj in ii] for ii in template]
  # Now we have newheader and newmeta along with mytemplate which will direct 
  # the extraction of each subsequent line of data.
  # Write them out to the output file
  fw.writerow(newhead)
  fw.writerow(newmeta)
  # Process each line of input and write it to the output following the 
  # guidance of the template
  #import pdb; pdb.set_trace()
  for linein in fr:
    lineout = []
    # there are as many items (ii) in template as there are input columns
    for ii in range(0,ncols):
      # each item (ii) in template contains one or more specifications (jj)
      # for output columns, with each jj specifying a different output
      # column derived from the current ii
      for jj in template[ii]:
	# and in each jj first element is the name of the set of arguments that
	# will be passed to xfieldj() for it to produce the appropriate format
	# of output. THIS IS WHERE THERE NEED TO BE ADDITIONAL ENTRIES IN ORDER
	# TO HAVE MULTIPLE DERIVED COLUMNS FOR THE SAME VARIABLE
	if(jj[0] != 'skip'):
	  lineout.append(xfieldj(linein[ii],**testargs[jj[0]]))
    fw.writerow(lineout)
  inhandle.close(); outhandle.close();

if __name__ == '__main__':
    outfile = args.outfile
    # TODO: fix this path-unaware file name generator
    #if outfile=="":
    #  outfile = "data_"+args.csvin
    if path.isfile('testinput.py'):
      from testinput import testheader,testmeta,testqb #,testUserRule
      from rules import rules2
      dfm = DFMeta('../www/demodata.csv'
	#,testheader,testmeta
	,suggestions=autosuggestor)
      
      # comparison of two different methods of dfmeta creation
      dfm1 = DFMeta('exampleinput.db')
      dfm0 = DFMeta('exampleinput.csv')
      try:
	# only needed for testing, not intended to be part of this package
	from deepdiff import DeepDiff
	dfmDiff = DeepDiff(dfm0,dfm1)
	if dfmDiff: print '''
	Have a look at dfmDiff, something is different between dfm0 (csv input)
	and dfm1 (db input but identical data)'''
      except: pass
    
      try: dfc = dfm.incols['v113_RDW_RBC_At_Rt'] #['v036_CS_Mts_at_DX']
      except: dfc = dfm.incols['v006_Hrt_Rt_LNC']
      colids=dfc.getColIDs(childids=['selid','addbid','shortname','longname','ruledesc','parent_name']
		    ,childtype='rules')
      
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
      testDelim10 = handleDelimFile('tenlines.csv')
      testDelim3 = handleDelimFile('threelines.csv')
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
      #bat = {'v006_Mlgnt_nplsm':["v006_cd","v006_tf"]}
      #dfm.finalizeChosen(bat)
      import pdb; pdb.set_trace()
    #update_df(args.csvin)
