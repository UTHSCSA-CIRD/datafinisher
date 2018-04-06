import sqlite3 as sq,argparse,re,csv,time,ConfigParser,pdb
import json, sys
from os.path import dirname
cwd = dirname(__file__)
if cwd == '': cwd = '.'
# okay, below looks screwed up because it seems like a circular reference
# but it does the job of communicating to the functions in this module whether or
# not the user wants verbose logging
from df import dolog

# useful lists
# columns that may affect the interpretation of the data
cols_obsfact = ['instance_num','modifier_cd','valtype_cd','tval_char','valueflag_cd','quantity_num','units_cd','location_cd','confidence_num'];
cols_patdim = ['birth_date','sex_cd','language_cd','race_cd'];
cols_rules = ['sub_slct_std','sub_payload','sub_frm_std','sbwr','sub_grp_std','presuffix','suffix','concode','rule','grouping','subgrouping','in_use','criterion'];
# the columns to pull (from df_dynsql) to create the data dictionary file
cols_meta = ['colname', 'colname_long', 'rule'];

###############################################################################
# Functions and methods to use within SQLite                                  #
###############################################################################

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

###############################################################################
# Functions used in df.py directly                                            #
###############################################################################

def xfieldj(data, field, transform=None, select=None, sep='; ', omitnull=True, as_is=False, *args, **kwargs):
  """
  The data argument should be a string in JSON format that contains one or
  more JSON objects. The fields should be a named field in those objects.
  If transform is None, a list of values is returned, otherwise transform
  is first applied to it, and should be a function, which could be an
  aggregation function.
  
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
  if(as_is) return(data)
  if(data in ['',None]): return('')
  # TODO: return malformed json as_is for the user to figure out? Perhaps if debug is enabled?
  unpdat = json.loads(data)
  # notice that we wrap in sorted() because dicts have an undefined order
  oo = [unpdat[xx].get(field,None) for xx in sorted(unpdat.keys()) if xx != 'count']
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
  if(callable(transform)): oo = transform(oo,*args,**kwargs)
  return(sep.join(set(oo)))

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

"""
Dynamic SQLifier?
"""
# should be easy to turn into aggregator UDF: just collect the args, and run ds* at the end

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
