import sqlite3 as sq,argparse,re,csv,time,ConfigParser,pdb
import json, sys
from os.path import dirname
cwd = dirname(__file__)
if cwd == '': cwd = '.'
# okay, below looks screwed up because it seems like a circular reference
# but it does the job of communicating to the functions in this module whether or
# not the user wants verbose logging
#try: from df import dolog
#except: dolog = False
dolog = False

# a configuration-like object where all the rules are defined-- what patterns
# to look for in the JSON fields and what extractors and names to return for
# each pattern
from rules import rules,rules2,autosuggestor

# useful listsp
# columns that may affect the interpretation of the data
cols_obsfact = ['instance_num','modifier_cd','valtype_cd','tval_char'
		,'valueflag_cd','quantity_num','units_cd','location_cd'
		,'confidence_num'];
cols_patdim = ['birth_date','sex_cd','language_cd','race_cd'];
cols_rules = ['sub_slct_std','sub_payload','sub_frm_std','sbwr','sub_grp_std'
	      ,'presuffix','suffix','concode','rule','grouping','subgrouping'
	      ,'in_use','criterion'];
# the columns to pull (from df_dynsql) to create the data dictionary file
cols_meta = ['colname', 'colname_long', 'rule'];

###############################################################################
# Functions and methods to use within SQLite                                  #
###############################################################################
#section python_udf

# aggregator useful for generating SQL
class sqlaggregate:
  def __init__(self):
    self.lvals = []; self.rvals = []
    self.lfuns = []; self.rfuns = []
    self.ops = []; self.joiner = ','
  def step(self,lval,rval,lfun,op,rfun,joiner):
    if lval in ['','None',None]: lval = ' '
    if rval in ['','None',None]: rval = ' '
    if lfun in ['','None',None]: lfun = ' {0} '
    if rfun in ['','None',None]: rfun = ' {0} '
    if op in ['','None',None]: op = ' '
    if joiner in ['','None',None]: self.joiner = ','
    else: self.joiner = joiner
    self.lvals.append(lval)
    self.rvals.append(rval)
    self.lfuns.append(lfun)
    self.rfuns.append(rfun)
    self.ops.append(op)
  def finalize(self):
    # turn into tuples
    rawvals = zip(self.lfuns,self.lvals,self.ops,self.rfuns,self.rvals);
    # payload
    out = [str(xx[0]).format(str(xx[1]))+\
      str(xx[2])+str(xx[3]).format(str(xx[4])) for xx in rawvals]
    return self.joiner.join(out)

# aggregation for diagnoses and similar data elements
class diaggregate:
  def __init__(self):
    self.cons = {}
    self.oocm = {}; self.ooc = []
  def step(self,con,mod):
    if con not in self.cons.keys():
      self.cons[con] = [mod]
    else:
      if mod not in self.cons[con]:
	self.cons[con].append(mod)
  def finalize(self):
    for ii in self.cons:
      iimods = [jj for jj in self.cons[ii] if jj not in  ['@',None,'']]
      if len(iimods) == 0:
	self.ooc.append('"'+ii+'"')
      else:
	self.oocm[ii] = iimods
    #oo += ['"'+ii+'":["'+'","'.join(self.cons[ii])+'"]' for ii in self.cons]
    #oo = ",".join(oo)
    return ",".join(self.ooc+['"'+ii+'":["'+'","'.join(self.oocm[ii])+'"]' for ii in self.oocm])
  
# generically jam together the ancillary fields to see if there is anything 
# noteworthy anywhere in there note that normally you would use NULL or '' for 
# some of these params (to bypass them), doing the aggregation only on the ones 
# you don't expect to see
class infoaggregate:
  def __init__(self):
    self.cons = {}
  def step(self,con,mod,ins,vtp,tvc,nvn,vfl,qty,unt,loc,cnf):
    self.ofvars = {'cc':str(con),'mc':str(mod),'ix':str(ins),'vt':str(vtp),'tc':str(tvc),'vf':str(vfl),'qt':str(qty),'un':str(unt),'lc':str(loc),'cf':str(cnf)}
    # go through each possible arg, check if it's NULL/@/''
    # if not, add to self.cons
    if nvn not in ['@','None',None,'']:
      if 'nv' not in self.cons.keys():
	self.cons['nv'] = 1
      else:
	self.cons['nv'] += 1
    for ii in self.ofvars:
      if self.ofvars[ii] not in ['@','None',None,'']:
	if ii not in self.cons.keys():
	  self.cons[ii] = [self.ofvars[ii]]
	elif self.ofvars[ii] not in self.cons[ii]:
	  self.cons[ii] += [self.ofvars[ii]]
  def finalize(self):
    # oh... python's dictionary format looks just like JSON, and you can convert it to a string
    # the replace calls are just to make it a little more compact
    if 'nv' in self.cons.keys():
      if self.cons['nv']==1:
	del self.cons['nv']
      else:
	self.cons['nv'] = str(self.cons['nv'])
    if 'ix' in self.cons.keys():
      if len(self.cons['ix']) == 1:
      #if self.cons['ix'] == ['1']:
	del self.cons['ix']
    return (str(self.cons)[1:-1]).replace("', '","','").replace(": ",":")

# Packs a set of rows from i2b2 concept_dimension into a JSON object (string)
class jsonaggregate:
  def __init__(self):
    self.entries = {}
  def step(self,st,cc,mc,ix,vt,tc,nv,vf,qt,un,lc,cf):
    fields = vars()
    self.entries[len(self.entries)] = ({
      xx: fields.get(xx,None) if fields.get(xx,None) not in ['@',None,'','None'] else None 
      for xx in ('st','cc','mc','ix','vt','tc','nv','vf','qt','un','lc','cf')})
  def finalize(self):
    self.entries['count'] = len(self.entries)
    #import pdb; 
    #if(self.entries['count']>1): pdb.set_trace();
    return json.dumps(self.entries)

# this is the kitchen-sink aggregator-- doesn't really condense the data, 
# rather the purpose is to preserve everything there is to be known about 
# each OBSERVATION_FACT entry while still complying with the 
# one-row-per-patient-date requirement
class debugaggregate:
  def __init__(self):
    self.entries = []
  def step(self,cc,mc,ix,vt,tc,nv,vf,qt,un,lc,cf):
    foo = vars()
    bar = {xx: foo.get(xx,None) if foo.get(xx,None) not in ['@',None,'','None'] else None for xx in ('cc','mc','ix','vt','tc','nv','vf','qt','un','lc','cf')}
    import pdb; pdb.set_trace();
    self.entries.append(",".join(['"'+ii+'":"'+str(vars()[ii])+'"' for ii in ['cc','mc','ix','vt','tc','nv','vf','qt','un','lc','cf'] if vars()[ii] not in ['@',None,'','None']]))
  def finalize(self):
    return "{"+"},{".join(self.entries)+"}"

# trim and concatenate together strings, e.g. to make column names 
def trimcat(*args): return ''.join([ii.strip() for ii in args])
  
# from the template in the first argument ({0},{1}, etc.)
# and the replacement variables in the second, put together a string
# useful for generating SQL 
def pyformat(string,*args): return string.format(*args)

# this is to register a SQLite function for pulling out matching substrings 
# (if found) and otherwise returning the original string. Useful for extracting 
# ICD9, CPT, and LOINC codes from concept paths where they are embedded. For 
# ICD9 the magic pattern is:
# '.*\\\\([VE0-9]{3}\.{0,1}[0-9]{0,2})\\\\.*'
# Returns last match or original text if no match
def ifgrp(pattern,txt):
    #rs = re.search(re.compile(pattern),txt)
    rs = re.findall(re.compile(pattern),txt)
    if len(rs):
      rs = rs[-1]
      if isinstance(rs,tuple): return rs[0]
      else: return rs
    else:
      return txt 
    #else:
    #  return rs.group(1)
    
def subgrp(pattern,rep,txt):
  return re.sub(pattern,str(rep),str(txt))

# The rdt and rdst functions aren't exactly user-defined SQLite functions...
# They are python functions that emit a string to concatenate into a larger SQL query
# and send back to SQL... because SQLite has a native julianday() function that's super
# easy to use. So, think of rdt and rdst as pseudo-UDFs
def rdt(datecol,factor):
    if factor == 1:
      return 'date('+datecol+')'
    else:
      factor = str(factor)
      return 'date(round(julianday('+datecol+')/'+factor+')*'+factor+')'
    
# this one is a wrapper for rdt but with 'start_date' hardcoded as first arg
# because it occurrs so often
def rdst(factor):
    return rdt('start_date',factor)

# Next two are more pseudo-UDFs, that may at some point be used by dd.sql
def dfctday(**kwargs):                                          
  if kwargs is not None:
    oo = "replace(group_concat(distinct '{'||"
    for key,val in kwargs.iteritems():
      oo += """coalesce('{0}:"'||{1}||'",','')||""".format(key,val)
    oo += "'}'),',}','}')"                                             
    return oo
  
def dfctcode(**kwargs):
   if kwargs is not None:
     oo = ""
     for key,val in kwargs.iteritems():
       oo += """coalesce('{0}:['||group_concat(distinct '"'||{1}||'"')||'],','')||""".format(key,val)
     return oo[:-2].replace('],',']')

# Omit "least relevant" words to make a character string shorter
def shortenwords(words,limit):
  """ Initialize the data, lengths, and indexes"""
  #get rid of the numeric codes
  words = re.sub('[0-9]','',words)
  wrds = words.split(); lens = map(len,wrds); idxs=range(len(lens))
  if limit >= len(words):
    return(words)
  """ sort the indexes and lengths"""
  idxs.sort(key=lambda xx: lens[xx]); lens.sort()
  """ initialize the threshold and the vector of 'most important' words"""
  sumidx=0; keep=[]
  # turned out that checking the lengths of the lens and idxs is what it takes to avoid crashes
  while sumidx < limit and len(lens) > 0 and len(idxs) > 0:
    sumidx += lens.pop()
    keep.append(idxs.pop())
  keep.sort()
  shortened = [wrds[ii] for ii in keep]
  return " ".join(shortened)

# This function shortens words by squeezing out vowels, most non-alphas, and 
# repeating letters the first regexp replaces multiple ocurrences of the same 
# letter with one ocurrence of that letter the \B matches a word boundary... 
# so we only remove vowels from inside words, not leading lettters
def dropletters(intext):
  return re.sub(r"([a-z_ ])\1",r"\1",re.sub("\B[aeiouyAEIOUY]+","",re.sub("[^a-zA-Z _]"," ", intext)))

#end_section python_udf


###############################################################################
# Functions used in df.py directly                                            #
###############################################################################

### for json parsing 
#section json

class DFMeta: 
  '''Initialize with a list of column names and metadata 
  some of which is assumed to be strings convertible to dicts by wy of JSON
  
  suggestPolicy: if 'no' then ignore suggestions completely
		 if 'yes' then always create suggested columns unless 
		 conflicting with user choices
		 if 'auto' only create suggested columns if the user made no 
		 choices at all (default)
  
  Future plans: allow the first argument to be a file-handle
  '''
  def __init__(self,inhead,inmeta=None,suggestPolicy='auto'
	       ,rules=rules2,suggestions=None):
    if inmeta == None:
      inmeta = ['---']*len(inhead)
    assert len(inhead) == len(inmeta), '''
    'inhead' and 'inmeta' args to DFMeta() must be same length'''
    self.inhead = inhead
    self.inmeta = inmeta
    self.suggestPolicy = suggestPolicy
    self.rules = rules
    #self.incols = {kk: {
      #'dat': vv,'outcols':[{'cname':kk,'extr':'as_is'
			      #,'dat':json.dumps(vv) if isinstance(vv,dict) else vv
			      #,'args':[]}]
      #} for kk,vv in zip(self.inhead,[json.loads(jj) 
				      #if re.match('\{.*\}$',str(jj)) 
				      #else jj for jj in self.inmeta])}
    self.incols = {}
    # correct the input headers with what the metadata actually names them as
    #for kk,vv in zip(self.inhead,self.inmeta):
    for ii in range(len(self.inhead)):
      #kk_as_is_col = (re.match('\{.*\}$',str(vv))==None)
      iiname = self.inhead[ii]; iimeta = self.inmeta[ii]
      if iimeta == None: iimeta = ''
      ii_as_is_col = (re.match('\{.*\}$',str(iimeta))==None)
      # DFCols for static input columns
      if ii_as_is_col: #kk_as_is_col
	self.incols[iiname] = DFCol(iimeta,iiname,as_is_col=ii_as_is_col)
      # DFCols for non-static input columns
      else:
	iimeta = json.loads(iimeta)
	iiname = iimeta['colid']
	self.inhead[ii] = iiname
	self.incols[iiname] = DFCol(iimeta,iiname,as_is_col=ii_as_is_col)
    
    self.updRules(rules=self.rules,suggestions=suggestions)
    
    
  def updRules(self,rules=None,suggestions=None):
    '''Update with a new ruleset, optionally with suggestion algorithm'''
    if rules == None:
      rules = self.rules
    else: 
      self.rules = rules
	
    for ii in self.incols:
      self.incols[ii].updRules(rules,suggestions)
    return self
  
  def updSuggestions(self,suggestions):
    '''Update with a new suggestion algorithm, not needed if already passed algorithm to updRules'''
    for ii in self.incols:
      self.incols[ii].updSuggestions(suggestions)
    return self
  
  def updChoices(self,choices):
    '''For each of the incols do foo.updChoices(choices)'''
    pass
  
  def get(self, key, fallback=None):
    if(key in self.__dict__):
      return getattr(self,key)
    elif(key in self.incols):
      return self.incols[key]
    else: return fallback
  
  def __getitem__(self,key):
    if(key in self.__dict__):
      return getattr(self,key)
    if(key in self.incols):
      return self.incols[key]

  
  def getDict(self):
    return vars(self)
    
  def getHeaders(self,bycol=False,cols=None,*args,**kwargs):
    '''For each of the incols, do foo.getHeader() with the above arguments
    and in addition whatever the current value of suggestPolicy is
    
    cols: optional list of master column names in the order they should appear
    bycol: return output as a dictionary broken up by columns?
    '''
    if cols == None: cols = self.inhead
    # any arguments other than 'cols' get passed to the getHeader()
    # method of each of the self.incols
    # if breaking up by columns... dictionary keyed on those column names
    # Without any args/kwargs, this produces the output that may end up 
    # being the user intput message format
    if bycol:
      return {ii: self.incols[ii].getHeader(suggestPolicy=self.suggestPolicy,*args,**kwargs) for ii in cols}
    # return as unified list or dict
    out = [self.incols[ii].getHeader(suggestPolicy=self.suggestPolicy,*args,**kwargs) for ii in cols]
    # if out consists of lists... (as detected from its first element)
    if type(out[0]) == type([]):
      # flatten them, returning a list of dictionaries each specifying one 
      # output column
      return [ii for jj in out for ii in jj]
    # if out consists of dicts... (as detected from its first element)
    elif type(out[0]) == type({}):
      # iterate over the contributions of all the incols except the first
      for ii in range(1,len(out)):
	# and extend the lists from all of them to the corresponding lists
	# in the first in the order they appear (in the cols argument if 
	# provided)
	for jj in out[0].keys():
	  out[0][jj] += out[ii][jj]
      # and return a dictionary of lists, each list representing that 
      # attribute from all the columns, all of them having the same length
      # as the number of output columns, and in the same order
      return out[0]
  
  def processRow(self,cells,pidname='PATIENT_NUM'):
    '''For each of the incols, do foo.processCell() passing each one its cell
    and the pid, obtained by extracting the value specified by 'pidname'
    '''
    pass
    
''' Note: colmeta is a dict with the following fields:
 (see sql/dd.sql)
 "nval_num": 
 "patvis": 
 "patvis_null": 
 "pats": 
 "pats_null", 
 "confidence_num": 
 "mxconmod": 
 "done": 
 "colid":		the name assigned to this base column
 "concept_path": 
 "ddomain": 
 "valueflag_cd": 
 "tval_char": 
 "valtype_cd": 
 "mxinsts": 
 "ccd_list": 
 "mod": 
 "name": 
 "cid": 
 "colcd": 
 "rule": 
 "mxfacts": 
 "quantity_num": 
 "ccd": 
 "units_cd": 
 "location_cd":
'''

class DFCol:
  ''' Everything this column needs to know should be contained in the colmeta
  '''
  def __init__(self,colmeta,colname,rules=rules2,suggestions=None,as_is_col = False):
    self.colmeta = colmeta; self.incolid = colname; self.as_is_col = as_is_col;
    '''This is for later, to enable last-observation carry-forward extractors
    It compares current pid to previous so the carry-forward can be 
    restarted when the records for a new patient begin.
    '''
    self.last_pid = None
    # The set of derived columns chosen by the user and by suggestions
    self.chosen = []; self.suggested = []
    """This is the info column, which should be a replica of the column in 
    the input CSV file that produced this column"""
    self.dfcol = [{'cname':colname,'args':{}}]
    # info column for non-dynamic NON persistent columns, i.e. these will 
    # get blown away in the output
    if colmeta == None or colmeta == '':
      self.incoldesc = ''' This column was automatically generated by a previous run and will be overwritten. To keep this column as a static column give it any non-null value in the second row'''
      self.dfcol[0].update({'extr':'skip','rulename':'skip','colmeta':None
			   ,'ruledesc':self.incoldesc})
    # info column for the dynamic case and static persistent columns
    else: 
      self.dfcol[0].update({'extr': 'as_is','rulename': 'as_is'
			   ,'colmeta':json.dumps(colmeta) 
			   if isinstance(colmeta,dict) else colmeta
			   ,'ruledesc':''})
      if self.as_is_col:
	self.incoldesc = 'This is a static column that will be preserved as-is'
	self.short_incolid = self.incolid
      else:
	self.incoldesc = self.colmeta['name']
	self.short_incolid = self.colmeta['colcd']
	
    self.divIDchosen = 'c-'+self.short_incolid
    self.divIDavailable = 'a-'+self.short_incolid
    ''' Special case for static columns or ones with missing ccd_lists
    (due to having too many concept codes for example'''
    if self.as_is_col or self.colmeta['ccd_list'] == None:
      self.unique_codes = ['']
    else:
      # all distinct concept codes in this column, if there are not too many
      self.unique_codes = self.colmeta['ccd_list'].split(',')
      
    # Of the rules available, the ones that are valid for this column
    self.updRules(rules,suggestions)
    #import pdb; pdb.set_trace()
    #foo = self.runRule(self.rules['true_false'])
  
  def updRules(self,rules=rules2,suggestions=None):
    '''Replace the current rules with subset of new ones that are valid 
    for this columnn based on their built-in validity checks and colmeta
    
    If 'suggestions' argument provided, also updates suggestions
    '''
    
    # for static columns
    if self.as_is_col: 
      self.rules = {}
      return self
  
    rules0 = {kk: vv for kk,vv in rules.items() if eval(vv.get('criteria'),self.colmeta)}
    for ii in rules0: 
      rules0[ii]['suggested'] = False
      rules0[ii]['parent_name'] = self.incolid
      rules0[ii]['shortname'] = self.short_incolid+'_'+rules0[ii]['rulesuffix']
      rules0[ii]['longname'] = self.incolid+'_'+rules0[ii]['rulesuffix']
      rules0[ii]['addbid'] = 'ab-'+rules0[ii]['shortname']
      rules0[ii]['selid'] = 'sl-'+rules0[ii]['shortname']
    self.rules = rules0
    if suggestions != None: self.updSuggestions(suggestions)
    return self
    # initialize the 'suggested' attribute to 'False'
    
  def updSuggestions(self,suggestions):
    '''Update the 'suggested' attribute for each rule based on suggestions
    and replace self.suggested accordingly
    
    suggestions is a list of dicts
    '''
    # TODO: validate suggestions before proceeding
    # empty out the current auto-generated output columns

    # for static columns
    if self.as_is_col: return self

    self.suggested = []
    noutputs = 0
    for ii in suggestions:
      if ii.keys()[0] in self.rules:
	check = eval(ii.values()[0],locals(),self.colmeta)
	self.rules[ii.keys()[0]]['suggested'] = check
	if check:
	  self.suggested+=self.runRule(ii.keys()[0])
	noutputs += check
    return self
  
  def runRule(self,rule):
    ''' 
    rule: the NAME of a rule in self.rules
    
    return 'cname','extr','colmeta' (None),'args' {}
    if split_by_code, repeat this for every code in self.colmeta.ccd_list
    '''
    assert rule in self.rules, "Not the name of a valid rule"
    out = []
    if self.rules[rule]['split_by_code']:
      whatcode = self.unique_codes.split(',')
    else: whatcode = ['']
    #for ii in whatcode:
      #for jj in self.rules[rule]['extractors']:
	#coltmpl = jj[1]
	#if ii != '': 
	  #coltmpl = ii+'_'+coltmpl
	#out.append({'cname':coltmpl.format(self.name)
	     #,'extr':jj[0],'rulename':rule
	     #,'ruledesc':self.rules[rule].get('ruledesc','')
	     #,'colmeta':''
	     #,'args':{'whatcode':ii}})
    return out
	  
  
  def updChoices(self,choices):
    '''Get the user choices (extractors, names (?), and args) 
    and replace self.chosen accordingly.'''
    return self
  
  def get(self,key,fallback=None):
    if(key in self.__dict__):
      return getattr(self,key)
    else: return fallback
  
  def __getitem__(self, key): return getattr(self,key)
  
  def getDict(self):
    return vars(self)
  
  def getHeader(self
    ,fields=['cname','rulename','ruledesc','extr','colmeta','args']
    ,form='dicts',suggestPolicy='auto'):
    '''Return the fields specified by the 'fields' argument
    
    form:      if 'lists' returns each requested field as a list
	       if 'dicts' returns list of dicts (all fields)
	       (below not being implemented for now)
               ~~if 'zip' returns tuples of fields, one for each column~~
               
    Note: NEED TO MAKE SURE getHeader and processCell return in the same order!
    '''
    if suggestPolicy == 'yes' and len(self.chosen) == 0: sugg = self.suggested
    elif suggestPolicy == 'auto': 
      sugg = [xx for xx in self.suggested 
	      if xx.get('cname') not in [yy.get('cname') for yy in self.chosen]]
    else: sugg = []
    
    header = self.chosen + sugg + self.dfcol
    if form == 'lists':
      out = {}
      for ii in fields:
	out[ii]=[jj.get(ii) for jj in header]
      return out
    elif form== 'dicts':
      return header
    else:
      return None


  def processCell(self,celldata,pid=None,suggestPolicy='auto'):
    '''Iterates over each of {self.dfcol,self.chosen} (and, depending on
    suggestPolicy, self.suggested) and uses the 'extr' and 'args' values
    they contain to create and return a list of output of the same length
    '''
    pass
    
# Q: how to make this specify multiple output columns?
# A?: have a rule with multiple items for an extractor?

def rulesvalidate(datadict,rules=rules,recommendfield='recommend',*args, **kwargs):
  """
  * datadict: ONE valid dict object (no JSON, no nulls)
  * rules: a rules object, imported from rules.py by default
  * recommendfield: either empty string or the name of a field to look for in rules
  
  returns a boolean list same length as rules
  """
  # eval the criterion field of each rule for datadict
  out = [eval(xx.get('criteria','False'),datadict) for xx in rules];
  # if recommendfield is not empty then also evaluate the that field and AND it with previous result
  if(recommendfield != None and len(recommendfield)>0):
    out = [all(yy) for yy in zip([
      eval(xx.get(recommendfield,'False'),datadict) for xx in rules
      ],out)];
  return(out);
  # return list

def rulesselected(datadict,rules=rules,selected=[],*args, **kwargs):
  """
  * datadict: ONE valid dict object (no JSON, no nulls)
  * rules: a rules object, imported from rules.py by default
  * selected: a list of names or booleans (all the same type)
  
  returns a list containing at least one list (one for each output column). 
  These inner lists each have 3 values in the following order: 
  
    1. extractor name (what extractor function xfieldj() should call)
       Special values: 'as_is', 'skip'
    2. header (what the output column should be named, or empty string)
    3. value to place in the second row (metadata) 
  """
  # is selected empty? if so, run rulesvalidate and use that
  if(selected == None or length(selected)==0): 
    selected = rulesvalidate(datadict,rules,**kwargs);
  # get types of selected argument
  seltypes = [type(ii) for ii in selected];
  # is selected all boolean? if so, check that it has same length as rules
  if (all([ii == type(True) for ii in seltypes])):
    if (len(selected)==len(rules)):
      # if so, subset the rules accordingly
      selrules = [jj for ii,jj in zip(selected,rules) if ii];
    # but if length mismatch raise an error
    else: raise ValueError("""
      If 'selected' argument is boolean it must be the same length as the 'rules' argument
      """);
  # or is selected a list of all strings?
  elif (all([ii == type(True) for ii in seltypes])):
      # if so, select rules whose 'name' attributes match an item on the list
      selrules = [ii for ii in rules if ii.get('name') in selected];
  # if neither all-boolean nor all-string, error
  else: raise ValueError("""
    The 'selected' argument must be a list of all boolean or all string values.
    If all boolean, it must be the same length as the 'rules' argument.
    """);
  # iterate over selrules and get the rulename and column name for each
  outextr = []; outhead = [];
  for xx in selrules:
    outextr += [yy[0] for yy in xx['extractors']];
    outhead += [yy[1].format(datadict['colid'],datadict['colcd']) for yy in xx['extractors']];
  # return a tuple with outextr (extractor name) and outhead (column name)
  # TODO: test and error if outhead is not unique
  return outextr,outhead;

def xmetaj(data,header,rules=rules,chosen=0):
  """
  * data and header are both character values (not lists)
      * data is a JSON string
      * header is the literal column header
  * rules is a list of dicts (see rules.py)
  * chosen is... not implemented?

  returns a list containing at least one list (one for each output column). 
  These inner lists each have 3 values in the following order: 
  
    1. extractor name (what extractor function xfieldj() should call)
       Special values: 'as_is', 'skip'
    2. header (what the output column should be named, or empty string)
    3. value to place in the second row (metadata) 
  """
  """A missing value in the meta row is interpreted as being dynamically 
  generatedand so is marked for skipping (because presumably it will be 
  re-generated). To override this behavior, just make the value in the second
  of the input file neither null nor JSON
  """
  if data in ('',None): return([['skip','','']])

  """Now we try to crudely pre-filter stuff that isn't properly formatted JSON
  data wrapped in str() to avoid errors from numeric values. Failure to match
  is interpreted as a static column not controlled by the metadata row at all.
  """
  #if data in (0,'---'): return([['as_is',header,data]]) # old version
  # TODO: insure that the static columns never collide with dynamically generated ones
  if not re.match('^\{.*\}$',str(data)): return([['as_is',header,data]])
  # now we try to parse the metadata json (the stuff that's in the second row)
  try: jdata = json.loads(data)
  # if parsing fails we fall back on treating it as a static column
  except: return([['as_is',header,data]])
  # does this go any further than the first successfully matching rule?
  for xx in rules:
    # if a selected str
    if eval(xx['criteria'],jdata):
      outextr = [yy[0] for yy in xx['extractors']]
      outhead = [yy[1].format(jdata['colid'],jdata['colcd']) for yy in xx['extractors']]
      outmeta = [None] * len(outextr)
      if 'as_is' in outextr:
	# if for some reason the rules already create an as_is column, use that one
	# trust whatever the value for the header is, and the only thing needing to
	# change is the meta column which will be this one
	outmeta[outextr.index('as_is')] = data
      else:
	# otherwise we will need to tack one one (this time to each of the lists)
	# ...so that we preserve the meta data in the output for next time it needs
	# to be reorganized by the user
	outextr += ['as_is']
	outhead += [header]
	outmeta += [data]
      return(zip(outextr,outhead,outmeta))
  # if values are chosen for this column already, return those and nulls/data as meta
  # extract all values from data
  # iterate over the rules until one matches
  # return that rule's extractors, construct header, and nulls/data as meta

def xfieldj(data, field, transform=None, select=None, sep='; ', omitnull=True, as_is=False
	    , nulls_r_false=False, *args, **kwargs):
  """
  The data argument should be a string in JSON format that contains one or
  more JSON objects. The fields should be a named field in those objects.
  If transform is None, a list of values is returned, otherwise transform
  is first applied to it, and should be a function, which could be an
  aggregation function.
  TODO: transform should be a separate step in the chain?
  
  All of the following work (of course for other fields than 'ix' also:
  
  # using a function as the select argument
  xfieldj(testjson,'ix',None,lambda xx,mn: [90000000000000000>xx[zz].get('ix')>mn for zz in xx if zz !='count'],mn=10000000000000000)
  # return the max, min
  xfieldj(testjson,'ix',max); xfieldj(testjson,'ix',min); 
  # return the last, first
  xfieldj(testjson,'ix',lambda xx: xx.pop()); xfieldj(testjson,'ix',lambda xx: xx[0])
  # return any, all
  xfieldj(testjson,'ix',any); xfieldj(testjson,'ix',all)
  # median, mean, sd
  xfieldj(testjson,'ix',numpy.percentile,q=0.5);xfieldj(testjson,'ix',numpy.mean);
  xfieldj(testjson,'ix',numpy.std); 
  # random
  xfieldj(testjson,'ix',random.choice); 
  """
  if(as_is): return(data)
  if(data in ['',None]): 
    if(nulls_r_false): return(False)
    else: return('')
  # TODO: return malformed json as_is for the user to figure out? Perhaps if debug is enabled?
  unpdat = json.loads(data)
  # right now unpdat is a dict of dicts, so it's unordered and yet it might matter in which order 
  # the observations were entered, so we extract the keynames except count
  dkeys = [ii for ii in unpdat.keys() if ii != 'count']
  # df created all of these as integers, and we sort them as such without changing their values
  # or types
  dkeys.sort(key=int)
  # and now we turn unpdat into a list of dicts, with the same order as that of the original entries
  unpdat = [unpdat[ii] for ii in dkeys]
  oo = [xx.get(field,None) for xx in unpdat]
  # if a selection criterion is given, use it
  if(select != None): 
    if(callable(select)):
      select = select(unpdat,*args,**kwargs)
    err = False
    if(not isinstance(select,list)): err = True
    else:
      if(len(select) != len(oo)):
	err = True
    if(err):
      raise ValueError("The select argument should either be a list or a function that returns a boolean list (of the same length as the initial result extracted from the data)")
    select = [bool(xx) for xx in select]
    oo = [ii for (ii,jj) in zip(oo,select) if jj]
  if(omitnull): oo = [xx for xx in oo if xx is not None]
  #import pdb; pdb.set_trace()
  if(callable(transform)): oo = transform(oo,*args,**kwargs)
  if type(oo) in (str,unicode): return(oo)
  try:
    iter(oo)
    oo = sep.join(set(oo))
    return(oo)
  except:
    return(oo)
  
### end JSON parsing ###
#end_section json

def logged_execute(cnx, statement, comment=''):
    if dolog:
        if comment != '':
            print 'execute({0}): {1}'.format(comment, statement)
        else:
            print 'execute: {0}'.format(statement)
    return cnx.execute(statement)

def cleanup(cnx):
    df_stuff = """select distinct name from sqlite_master where type='{0}' and name like 'df_%'"""
    print "Dropping views"
    # below two lines still here for legacy reasons-- remove in a week or two
    v_drop = ['obs_all','obs_diag_active','obs_diag_inactive','obs_labs','obs_noins','binoutput']
    [logged_execute(cnx,"drop view if exists "+ii) for ii in v_drop]
    [logged_execute(cnx,"drop view if exists "+ii[0]) for ii in \
      logged_execute(cnx,df_stuff.format('view')).fetchall()]    
    if len(logged_execute(cnx,"pragma table_info(df_dynsql)").fetchall()) >0:
      print "Dropping temporary tables"
      # note that because we're relying on df_dynsql in order to find the temporary tables, 
      # those have to be dropped before the persistent tables including df_dynsql get dropped
      [logged_execute(cnx,ii[0]) for ii in \
	logged_execute(cnx,"select distinct 'drop table if exists '||ttable from df_dynsql").fetchall()]
    print "Dropping tables"
    [logged_execute(cnx,"drop table if exists "+ii[0]) for ii in \
      logged_execute(cnx,df_stuff.format('table')).fetchall()]
    # also have to drop the finalouput and finaloutput2 tables
    # TODO: either consolidate these tables or rename them or otherwise make them 
    # follow the same patterns as the other tables
    logged_execute(cnx,"drop table if exists fulloutput")
    logged_execute(cnx,"drop table if exists fulloutput2")
    print "Dropping indexes"
    [logged_execute(cnx,"drop index if exists "+ii[0]) for ii in \
      logged_execute(cnx,df_stuff.format('index')).fetchall()]
    

################################################################################
# Custom class methods                                                         #
################################################################################
#section subsectionconfig
# returns a dictionary of name:value pairs for an entire section
# sort of like ConfigParser.defaults() but for any section
# still with final failover to DEFAULT but now you can use 
# this output as a vars argument to a get()
# def section(self,name='unknown'): return dict(self.items(name))
def subsection(self,name='unknown',sep='_',default='unknown'):
  # in summary, we take whatever section is named by the `default`
  # argument, update it with the base-name if any
  # update it with the actual name, and return that dictionary
  basedict = dict(self.items(default))
  if name == default: return basedict
  topdict = dict(self.items(name))
  if name.find(sep) < 1 :
    basedict.update(topdict)
    return basedict
  else : basename,suffix = name.split(sep,1)
  #import pdb; pdb.set_trace()
  #if 'presuffix' in basedict.keys() and basedict['presuffix'] != '':
    #setsuffix = True
  #else: setsuffix = False
  if basename in self.sections():
      # use the basename's items and override them with topdict
      basedict.update(dict(self.items(basename)))
  basedict.update(topdict)
  if('grouping' in topdict.keys() and topdict['grouping'] != '1'):
    basedict['presuffix'] = "_"+suffix
  else:
    basedict['suffix'] = "_"+suffix
  #if setsuffix:
    #basedict['suffix'] = "_"+suffix
  #else: basedict['presuffix'] = "_"+suffix
  return basedict
#end_section subsectionconfig

"""
Dynamic SQLifier?
"""
# should be easy to turn into aggregator UDF: just collect the args, and run ds* at the end
#section dynsqlifier

# the core function
def ds(lval,rval=' ',lfun=' {0} ',rfun=' {0} ',op=' ',joiner=','):
  # check for optional args and expand as needed
  if isinstance(lval,str): lval = [lval];
  else: lval = map(str,lval);
  ln = len(lval);
  # TODO: check for mismatched list lengths, non-lists, etc.
  # TODO: check for non-string arguments (catch and fix numeric)
  # DONE: check for non-string lists (catch and fix numeric)
  # DONE: make it so that if joiner is None, then don't join, just return list
  # (so that we can use it to combine conditions)
  # for any string args, turn them into lists and extend to same length
  if isinstance(rval,str): rval = [rval]*ln
  else: rval = map(str,rval);
  if isinstance(lfun,str): lfun = [lfun]*ln;
  else: lfun = map(str,lfun);
  if isinstance(rfun,str): rfun = [rfun]*ln;
  else: rfun = map(str,rfun);
  if isinstance(op,str): op = [op]*ln;
  else: op = map(str,op);
  # turn into tuples
  rawvals = zip(lfun,lval,op,rfun,rval);
  # payload
  out = [str(xx[0]).format(str(xx[1]))+\
    str(xx[2])+str(xx[3]).format(str(xx[4])) for xx in rawvals];
  if joiner is None:
    return out;
  else:
    return joiner.join(out);

# convenience wrappers

# for select and order-by clauses
def dsSel(lval,rval='',lfun=' {0} '):
  if lfun != ' {0} ' and rval == '': rval = lval;
  return ds(lval,rval,lfun);

# for where clauses and the 'on' clauses of join statements
def dsCond(lval,rval,joiner=' and ',op=' = ',lfun = ' {0} ',rfun=' {0} '):
  return ds(lval,rval,lfun,rfun,op,joiner);

# TODO: a general-case join wrapper
#end_section dynsqlifier


def tprint(str,tt):
    print(str+":"+" "*(60-len(str))+"%9.4f" % round((time.time() - tt),4))
      

# create the rule definitions table 
# TODO: document the purpose of each column in this table
def create_ruledef(cnx, filename):
	print filename
	logged_execute(cnx,"DROP TABLE IF EXISTS df_rules")
	logged_execute(cnx,"CREATE TABLE df_rules (sub_slct_std UNKNOWN_TYPE_STRING, sub_payload UNKNOWN_TYPE_STRING, sub_frm_std UNKNOWN_TYPE_STRING, sbwr UNKNOWN_TYPE_STRING, sub_grp_std UNKNOWN_TYPE_STRING, presuffix UNKNOWN_TYPE_STRING, suffix UNKNOWN_TYPE_STRING, concode UNKNOWN_TYPE_BOOLEAN NOT NULL, rule UNKNOWN_TYPE_STRING NOT NULL, grouping INTEGER NOT NULL, subgrouping INTEGER NOT NULL, in_use UNKNOWN_TYPE_BOOLEAN NOT NULL, criterion UNKNOWN_TYPE_STRING)")
	to_db = []
	with open(filename) as csvfile:
	  readCSV = csv.reader(csvfile, skipinitialspace=True)
	  for row in readCSV:
	      to_db.append(row)
	cnx.executemany("INSERT INTO df_rules VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db[1:])
	cnx.commit()
	
# read a config file subsection as specified by a delimited string
