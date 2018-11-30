'''
This is a collection of python dict objects telling the xmetaj() function what 
rules and titles to suggest under what circumstances

TODO: Enforce the following validation rule: extractors[1] should be unique

Q: Is the reason this is a list rather than a named dict is because order 
   matters?
'''

'''
For 'criteria' in 'values' in 'autosuggestor', the following variables are 
currently available in the data object (the JSON from the second row of the
CSV  input file):
 'patvis': number of patient-visits having a non-null value for this column
 'patvis_null': number of patient-visits loacking any values for this column
 'pats': number of patients who have at least one non-null value for this column
 'pats_null': number of patients lacking any values for this column
 'mxconmod': maximum number of concept-mod combinations for any visit
 'quantity_num'
 'mxinsts'
 'nval_num'
 'cid'
 'name'
 'colcd'
 'rule'
 'ddomain'
 'concept_path'
 'mxfacts'
 'done'
 'valueflag_cd'
 'ccd'
 'colid'
 'tval_char'
 'mod'
 'confidence_num'
 'units_cd'
 'location_cd'
 'valtype_cd']
'''

rules = [
   { # if this column has any numeric values return the last for each visit
     "name": "last_numeric"
     # The criteria will be executed by eval() in the context of the JSON 
     # metadata that ultimately originates from the df_dtdict
     ,"criteria":"nval_num > 0"
     # first value: name of extractor function, 
     # second value: template for naming column
     ,"extractors":[["last_numeric","{0}.last.num"]]
   }

  ,{ # if this column consists of only NULL and one other value
     "name": "true_false"
    ,"criteria": 'True' #"ccd <= 1" # later, check for > 1 unique concept|mod per visit
    ,"extractors":[["true_false","{0}.tf"]]
   }

  ,{ # if this column has codes (and really anything else)
     "name": "concat_unique"
    ,"criteria":"True"
    ,"extractors":[["concat_unique","{0}.values"]]
   }

]
  
rules2 = {
   'last_numeric': { 
     # if this column has any numeric values return the last for each visit
     # The criteria will be executed by eval() in the context of the JSON 
     # metadata that ultimately originates from the df_dtdict
     "criteria":"nval_num > 0"
    ,"split_by_code": False
     # first value: name of extractor function, 
     # second value: template for naming column
    ,"extractors":[["last_numeric","{0}.last.num"]]}
  ,'true_false': { 
    # if this column consists of only NULL and one other value
     "criteria": 'True'
    ,"split_by_code": False
    ,"extractors":[["true_false","{0}.tf"]]}
  ,"concat_unique": { 
    # if this column has codes (and really anything else)
     "criteria":"True"
    ,"split_by_code": False
    ,"extractors":[["concat_unique","{0}.values"]]}
}

''' These are criteria applied in addition to the rules ones to meet the higher
threshold for actually suggesting the use of these rules

These are a list rather than a dict because order matters (it represents 
precedence, facilitated by the planned noutputs variable)

noutputs is going to be a variable that tracks how many previous 
rules have already been suggested for this column
'''
autosuggestor = [
   {'last_numeric': 'ccd==1 & noutputs==0'}
  #,{'median_multicol': 'ccd>1 & noutputs==0'}
  ,{'true_false': 'ccd==1 & noutputs==0'}
  ,{'concat_unique': 'noutputs==0'}
    # TODO: multi-numeric for when ccd>1
]

simulated_choices = [
 {
  "colmeta": "", 
  "cname": "v025_Drvd_Dscrpt.values", 
  "extr": "concat_unique", 
  "args": {
   "whatcode": ""
  }
 }
]


''' The `extractors` dict is not currently supposed to be imported by anything, 
the active one is `testargs` in `dfx.py`. At this time, the below copy is just
here for reference
'''

extractors = {
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
