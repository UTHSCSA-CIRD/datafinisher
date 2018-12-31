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

'''
The extractor rules that operate on the actual data cells have the following variables available:

cell_field	source_field	type		comments
st		start_date	date	
cc		concept_cd	code		get vector from colmeta$ccd_list, split on comma
mc		modifier_cd	character	actually code, but value-vector not available yet
ix		instance_num	integer	
vt		valtype_cd	character	actually code, but value-vector not available yet
tc		tval_char	character	might turn out to be code
nv		nval_num	numeric	
vf		valueflag_cd	character	actually code, but value-vector not available yet
qt		quantity_num	numeric		Might be integer?
un		units_cd	character	actually code, but value-vector not available yet
lc		location_cd	character	actually code, but value-vector not available yet
cf		confidence_num	numeric	


...as filters...

list(
#list(name = 'st', type = 'date')
 list(name = 'cc', type = 'string', input = 'selectize', values=strsplit(t_dat$colmeta$ccd_list,',')[[1]]) 
,list(name = 'mc', type = 'string', input = 'text')
,list(name='ix',type='integer')
,list(name = 'vt', type = 'string', input = 'text')
,list(name = 'tc', type = 'string', input = 'text')
,list(name='nv',type='double')
,list(name = 'vf', type = 'string', input = 'text')
,list(name='qt',type='double')
,list(name = 'un', type = 'string', input = 'text')
,list(name = 'lc', type = 'string', input = 'text')
,list(name='cf',type='double')
)

'''

i2b2fields = ['cc','mc','ix','vt','tc','nv','vf','qt','un','lc','cf']

# these are evaluated in the scope of each top level item in a cell dict (if any) and return T/F on which to select items
selectors = {
   'all': lambda **kwargs: True
  ,'codeIn_CC': lambda cc,CC,**kwargs: cc in CC if cc else False
  ,'inactivDiag': lambda mc,**kwargs: mc in ['DiagObs:MEDICAL_HX','PROBLEM_STATUS_C:3','PROBLEM_STATUS_C:2'] if mc else False
  ,'activeDiag': lambda mc,**kwargs: mc not in ['DiagObs:MEDICAL_HX','PROBLEM_STATUS_C:3','PROBLEM_STATUS_C:2'] if mc else False
}

# these are just fields to extract for each selected item
fieldlists = {
   'numeric': ['nv']
  ,'code': ['cc']
  ,'codemod': ['cc','mc']
  ,'mod': ['mc']
  ,'all': ['cc','mc','ix','vt','tc','nv','vf','qt','un','lc','cf']
}

# these all take a list as input and return a scalar value as output
aggregators = {
   'last': lambda xx,**kw: xx[-1:]
  ,'first': lambda xx,**kw: xx[:1]
  ,'min': min
  ,'max': max
  ,'any': any
  ,'mean': lambda xx,**kw: sum(xx)/len(xx)
  ,'median': lambda xx,**kw: sorted(xx)[len(xx)/2]
  ,'concatunique': lambda xx,sep=';',**kw: sep.join([str(ii) for ii in xx])
}

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

# the following should be unique: rulesuffix, name of each rule
# if split_by_code there must be a {1} in the rulesuffix and args must have at least one value
rules2 = {
   'last_numeric': { 
     'ruledesc':'''Last numeric value for each visit'''
      # The criteria will be executed by eval() in the context of the JSON 
     # metadata that ultimately originates from the df_dtdict
     # must be a string
    ,"criteria":"nval_num > 0"
      # optional
    ,"split_by_code": False
     # first value: name of extractor function, 
     # second value: template for naming column
    #,"extractors":[["last_numeric","{0}_last_num"]]
    # must be a string or callable
    ,"selector": 'all'
      # must be a string or a list
    ,"fieldlist": ['nv']
    # must be a string or a callable
    ,"aggregator": 'last'
      # must be a string
    ,"rulesuffix": 'ln'
    # ignore these unless split_by_code is True
    ,"args": []
    }
  ,'last_numeric_fltrcode':{
    'ruledesc':'''Last numeric value of the specified code for each visit.'''
    # TODO: add whatever the variable where the number of distinct concept cds is stored
    ,"criteria": "nval_num > 0 and ccd > 1"
    ,"split_by_code": True
    #,"extractors":[["last_numeric","{0}_last_num_cd",{}]]
    ,"selector": 'codeIn_CC'
    ,"fieldlist": ['nv']
    ,"aggregator": 'last'
    ,"rulesuffix": 'lnc'
    ,"args": ['CC']
    }
  ,'true_false': { 
     'ruledesc':'''True if occurred during visit, otherwise false.'''
    ,"criteria": 'True'
    ,"split_by_code": False
    #,"extractors":[["true_false","{0}_tf"]]
    ,"selector": 'all'
    ,"fieldlist": 'all'
    ,"aggregator": 'any'
    ,"rulesuffix": 'tf'
    ,"args": []
    }
  ,"concat_unique": { 
     'ruledesc':'''All unique codes that correspond to this variable recorded during visit.'''
    ,"criteria":"True"
    ,"split_by_code": False
    #,"extractors":[["concat_unique","{0}_values"]]
    ,"selector": 'all'
    ,"fieldlist": ['cc']
    ,"aggregator": 'concatunique'
    ,"rulesuffix": 'cd'
    ,"args": []
    }
}
  
rules_fallback = {
  'ruledesc':'(not documented)'
  ,'criteria':'True'
  ,'split_by_code': False
  ,'selector':selectors['all']
  ,'selector_stronly':'all'
  ,'fieldlist':fieldlists['codemod']
  ,'aggregator': aggregators['concatunique']
  ,'aggregator_stronly':'concatunique'
  ,'args': []
  ,'suggested': False
  # rulesuffix needs to be unique, has to be set manually or be empty?
  # rulename needs to be passed in
  # short_incolid  needed to make shortname
  # 
}

''' These are criteria applied in addition to the rules ones to meet the higher
threshold for actually suggesting the use of these rules

These are a list rather than a dict because order matters (it represents 
precedence, facilitated by the planned noutputs variable)

noutputs is going to be a variable that tracks how many previous 
rules have already been suggested for this column
'''
autosuggestor = [
   {'last_numeric': 'ccd==1 and noutputs==0'}
  #,{'median_multicol': 'ccd>1 & noutputs==0'}
  ,{'true_false': 'ccd==1 and noutputs==0'}
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

#lambda cc=None,mc=None,ix=None,vt=None,tc=None,nv=None,vf=None,qt=None,un=None,lc=None,cf=None,**kwargs: cc in CC if cc else False


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
