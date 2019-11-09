""" Generate dynamic data extraction SQL for DataBuilder output files
---------------------------------------------------------------------
usage: df.py [-h] [-l] [-c] [-v CSVFILE] [-s {concat,simple}] [-d DATECOMPRESS] [-m MINIMUMCOUNT] dbfile
    
"""

import sqlite3 as sq,re,csv,time,ConfigParser,pdb,json #,argparse
from os.path import dirname,basename
from copy import deepcopy
from tempfile import mkstemp
from args import CONSOLE_ARGS

#section config
cwd = dirname(__file__)
if cwd == '': cwd = '.'
cfg = ConfigParser.RawConfigParser()
cfg.read(cwd+'/sql/df.cfg')
par=dict(cfg.items("Settings"))

# location of data dictionary sql file
ddsql = cwd + "/sql/dd.sql"
# TODO: make these passable via command-line argument for customizability
binvals = ['No','Yes']
# this says how many joins to permit per sub-table
joffset = 60

from df_fn import DFMeta,logged_execute,shortenwords,tprint,cleanup,ifgrp,subgrp,dropletters,pyformat,trimcat,diaggregate,infoaggregate,jsonaggregate,rdst,subsection,dsSel,rdt,cols_meta
from rules import autosuggestor,rules
#end_section config

def main(cnx,fname,style,dtcp,mincnt):
    tt = time.time(); startt = tt
    
    #section custom_functions
    """declare some custom functions to use within SQL queries (awesome!)"""
    #returns matching regexp if found, otherwise original string
    cnx.create_function("grs",2,ifgrp)
    # regexp replace... i.e. sed
    cnx.create_function("grsub",3,subgrp)
    # omit "least relevant" words to make a character string shorter
    cnx.create_function("shw",2,shortenwords)
    # shorten words by squeezing out certain characters
    cnx.create_function("drl",1,dropletters)
    # pythonish string formatting with replacement
    cnx.create_function("pyf",5,pyformat)
    cnx.create_function("pyf",4,pyformat)
    cnx.create_function("pyf",3,pyformat)
    cnx.create_function("pyf",2,pyformat)
    # trim and concatenate arguments
    cnx.create_function("tc",4,trimcat)
    cnx.create_function("tc",3,trimcat)
    cnx.create_function("tc",2,trimcat)
    # string aggregation specific for diagnoses and codes that behave like them
    #cnx.create_aggregate("dgr",2,diaggregate)
    # string aggregation for user-specified fields
    #cnx.create_aggregate("igr",11,infoaggregate)
    # the kitchen-sink aggregator that tokenizes and concatenates everything
    #cnx.create_aggregate("xgr",11,debugaggregate)
    cnx.create_aggregate("xgr",12,jsonaggregate)
    #cnx.create_aggregate("sqgr",6,sqlaggregate)
    #end_section custom_functions
    
    #section regexps
    """Hard-coded regexp strings for codes"""
    # TODO: this is a hardcoded dependency on LOINC and ICD9 strings in paths! 
    #       This is not an i2b2-ism, it's an EPICism, and possibly a HERONism
    #       Should be configurable!
    # regexps to use with grs SQL UDF (above)
    # not quite foolproof-- still pulls in PROCID's, so we filter for DX_ID
    # for ICD9 codes embedded in paths
    #icd9grep = '.*\\\\([VE0-9]{3}(\\.[0-9]{0,2}){0,1})\\\\.*'
    icd9grep = '\\\\(V0{0,1}\d{2}|V0{0,1}\d{2}\.\d{1,2}|\d{3}|\d{3}\.\d{1,2}|E\d{3}|E\d{3}\.\d{1,2})\\\\'
    icd9grepshort = '\\\\(V0?\d{2}|V0?\d{2}\.\d{1,2}|\d{3}|\d{3}\.\d{1,2}|E\d{3}|E\d{3}\.\d{1,2})\\\\'
    icd10grep = '\\\\([A-TV-Z][0-9][A-Z0-9](\.?[A-Z0-9]{0,4})?)\\\\'
    # for ICD9 codes embedded in i2b2 CONCEPT_CD style codes
    icd9grep_c = '^ICD9:([VE0-9]{3}(\\.[0-9]{0,2}){0,1})$'
    # for LOINC codes embedded in paths
    #loincgrep = '\\\\([0-9]{4,5}-[0-9])\\\\COMPONENT'
    loincgrep = '([0-9]{4,5}-[0-9])'
    # for LOINC codes embedded in i2b2 CONCEPT_CD style codes
    loincgrep_c = '^LOINC:([0-9]{4,5}-[0-9])$'
    #end_section regexps

    """ notes
    DONE (ticket #1): instead of relying on sqlite_denorm.sql, create the df_joinme table from inside this 
    script by putting the appropriate SQL commands into character strings and then passing those
    strings as arguments to execute() (see below for an example of cur.execute() usage (cur just happens 
    to be what we named the cursor object we created above, and execute() is a method that cursor objects have)
    
    DONE: create an id to concept_cd mapping table (and filtering out redundant facts taken care of here)
    """
    # TODO: parameterize the fact-filtering

    #section var_persistence
    # Variable persistence not fully implemented and this implementation might 
    # not be a good idea. If this block (through the "Uh oh...") isn't broken, 
    # ignore it for now. Ditto with datafinisher_log, but used even less.
    # create a log table
    logged_execute(cnx, """create table if not exists datafinisher_log as
      select datetime() timestamp,
      'FirstEntryKey                                     ' key,
      'FirstEntryVal                                     ' val""")
    # certain values should not be changed after the first run
    logged_execute(cnx, "CREATE TABLE if not exists df_vars ( varname TEXT, textval TEXT, numval NUM )")
    # TODO: oldtcp is a candidate for renaming
    olddtcp = logged_execute(cnx, "select numval from df_vars where varname = 'dtcp'").fetchall()
    if len(olddtcp) == 0:
      logged_execute(cnx, "insert into df_vars (varname,numval) values ('dtcp',"+str(dtcp)+")")
      cnx.commit()
      print "First run since cleanup, apparently"
    elif len(olddtcp) == 1:
      if dtcp != olddtcp:
	dtcp = olddtcp[0][0]
	print "Warning! Ignoring requested datecompress value and using previously stored value of "+str(dtcp)
	print "To get rid of it, do `python df.py -c dbfile`"
    else:
      print "Uh oh. Something is wrong there should not be more than one 'dtcp' entry in df_vars, debug time"
    #end_section var_persistence

    #section modifiers
    """Sooner or later we will need to write rules that make modifier codes human readable
    E.g.: allergies, family history. MODIFIER_DIMENSION has mappings for such codes. If 
    the site providing the databuilder file did not include any entries in its MODIFIER_DIMENSION
    we use our own, below."""
    if logged_execute(cnx, "select count(*) from modifier_dimension").fetchone()[0] == 0:
      print "modifier_dimension is empty, let's fill it"
      # we load our local fallback db
      logged_execute(cnx, "attach '{0}/sql/datafinisher.db' as dfdb".format(cwd))
      # and copy from it into the input .db file's modifier_dimension
      logged_execute(cnx, "insert into modifier_dimension select * from dfdb.modifier_dimension")
      # and log that we did so
      logged_execute(cnx, "insert into datafinisher_log select datetime(),'insert','modifier_dimension'")
      cnx.commit()
    #end_section modifiers

    # tprint is what echoes progress to console
    tprint("initialized variables",tt);tt = time.time()
    """ df_joinme has all unique patient_num and start_date combos, and 
    therefore it defines which rows will exist in the output CSV file. All 
    other columns that get created will be joined to it"""
    #section joinme
    logged_execute(cnx, par['create_joinme'].format(rdst(dtcp)))
    logged_execute(cnx, "CREATE UNIQUE INDEX if not exists df_ix_df_joinme ON df_joinme (patient_num,start_date) ")
    tprint("created df_joinme table and index",tt);tt = time.time()
    #end_section joinme


    # the CDID table maps concept codes (CCD) to variable id (ID) to 
    # data domain (DDOMAIN) to concept path (CPATH)
    #section codeid
    logged_execute(cnx, par['create_codeid_tmp'])
    logged_execute(cnx, "update df_codeid_tmp set display_code = grs('"+icd9grep+"',cpath) where display_code like '\i2b2\Diagnoses\ICD9\%'")
    logged_execute(cnx, "update df_codeid_tmp set display_code = grs('"+icd10grep+"',cpath) where display_code like '\i2b2\Diagnoses\ICD10\%'")
    logged_execute(cnx, "update df_codeid_tmp set display_code = ccd where length(display_code) > 30 and ddomain in ('ICD10','ICD9') and ccd like 'ICD%'")
    logged_execute(cnx, "update df_codeid_tmp set display_code = name_char where ddomain in ('UTHSCSA|FINCLASS','NAACCR','SEER_SITE','DEM|ETHNICITY','DEM|VITAL','DEM|VITAL|SSA','KUH|PAT_ENC')")
    logged_execute(cnx, par['create_codeid_displayfix'])
    #import pdb; pdb.set_trace()
    tprint("created df_codeid_tmp table",tt);tt = time.time()
    
    # Now we will replace the EHR-specific concept paths simply with the most 
    # granular available standard concept code (so far only for ICD9 and LOINC)
    # TODO: more generic compression of terminal code-nodes (RXNorm, CPT, etc.)

    # diagnoses
    logged_execute(cnx, "update df_codeid_tmp set cpath = grs('"+icd9grep+"',cpath) where ddomain like '%|DX_ID'")
    # TODO: the below might be more performant in current SQLite versions, might want to put it
    # back in after adding a version check
    # logged_execute(cnx, """update df_codeid set cpath = substr(ccd,instr(ccd,':')+1) where ddomain = 'ICD9'""")
    logged_execute(cnx, "update df_codeid_tmp set cpath = replace(ccd,'ICD9:','') where ddomain = 'ICD9'")
    # LOINC
    logged_execute(cnx, "update df_codeid_tmp set cpath = grs('"+loincgrep+"',cpath) where ddomain like '%|COMPONENT_ID'")
    # LOINC nodes modified analogously to ICD9 nodes above
    #logged_execute(cnx, """update df_codeid set cpath = substr(ccd,instr(ccd,':')+1) where ddomain = 'LOINC'""")
    logged_execute(cnx, "update df_codeid_tmp set cpath = replace(ccd,'LOINC:','') where ddomain = 'LOINC'")
    # several times now we have found bugs caused by cpaths remaining as paths becasue nothing catches the root concept codes.
    # Below specifically detects the rows where the cpath has original paths (or anything else that has characters illegal in a column name excep '-')
    # and replaces them with sanitized versions of the raw concept codes whatever they might be
    logged_execute(cnx, """update df_codeid_tmp set cpath = 'GENERIC_'||grsub('[^A-Za-z0-9_]','_',ccd) where ddomain not in ('ICD9','LOINC','NAACCR','SEER_SITE','ICD10','DEM|ETHNICITY','DEM|MARITAL','DEM|SEX','KUMC|FAMILYHISTORYDIAG','DEM|RACE','DEM|LANGUAGE') and cpath != grs('[^A-Za-z0-9_-]',cpath)""")
    # TODO: we need to find those and replace them with a sanitized version of the raw concept codes via grsub()
    # TODO: also, make sure that only CCDs that actually exist in obs_fact are included here
    # df_codeid gets created here from the distinct values of df_codeid_tmp
    logged_execute(cnx, par['create_codeid'])
    logged_execute(cnx, "create UNIQUE INDEX if not exists df_ix_df_codeid ON df_codeid (id,cpath,ccd)")
    logged_execute(cnx, "drop table if exists df_codeid_tmp")
    cnx.commit()
    tprint("mapped concept codes in df_codeid",tt);tt = time.time()
    #end_section codeid
    
    #section obsfact
    # The create_obsfact table may make most of the views unneccessary... it did!
    logged_execute(cnx, par['create_obsfact'].format(rdst(dtcp)))
    logged_execute(cnx, "create INDEX if not exists df_ix_obs ON df_obsfact(pn,sd,concept_cd,instance_num,modifier_cd)")
    cnx.commit()
    tprint("created df_obsfact table and index",tt);tt = time.time()
    #end_section obsfact
    
    #section rules
    # DONE: As per Ticket #19, this was changed so the rules get read 
    # in from ./ruledefs.csv and a df_rules table is created from it
    #create_ruledef(cnx, '{0}/{1}'.format(cwd, par['ruledefs']))
    #
    # we make the subsection() function declared in df_fn.py a 
    # method of ConfigParser
    ConfigParser.ConfigParser.subsection = subsection
    cnf = ConfigParser.ConfigParser()
    cnf.read('%s/sql/test.cfg' % cwd)
    ruledicts = [cnf.subsection(ii) for ii in cnf.sections()]
    # replacement for df_rules
    logged_execute(cnx,"""CREATE TABLE IF NOT EXISTS df_rules 
		    (sub_slct_std UNKNOWN_TYPE_STRING, sub_payload UNKNOWN_TYPE_STRING
		    , sub_frm_std UNKNOWN_TYPE_STRING, sbwr UNKNOWN_TYPE_STRING
		    , sub_grp_std UNKNOWN_TYPE_STRING, presuffix UNKNOWN_TYPE_STRING
		    , suffix UNKNOWN_TYPE_STRING, concode UNKNOWN_TYPE_BOOLEAN NOT NULL
		    , rule UNKNOWN_TYPE_STRING NOT NULL, grouping INTEGER NOT NULL
		    , subgrouping INTEGER NOT NULL, in_use UNKNOWN_TYPE_BOOLEAN NOT NULL
		    , criterion UNKNOWN_TYPE_STRING) """);
    #logged_execute(cnx,"delete from df_rules"); cnx.commit();
    # we read our cnf.subsection()s in...
    # populate the df_rules table to make sure result matches the .csv rules
    [cnx.execute("insert into df_rules ({0}) values (\" {1} \")".format(
      ",".join(ii.keys()),' "," '.join(ii.values()))) for ii in ruledicts if ii['in_use']=='1']
    tprint("created rule definitions",tt);tt = time.time()
    #end_section rules

    # Read in and run the sql/dd.sql file
    #section dynsql
    with open(ddsql,'r') as ddf:
	ddcreate = ddf.read();
    # cannot execute multiple statements, so we split the statements 
    ddcreate = ddcreate.split(';');
    # the production version shall always be the first statement
    logged_execute(cnx, ddcreate[0]);
    if par['debuglevel'] > 0:
      logged_execute(cnx,ddcreate[1]);
      assert (logged_execute(cnx,ddcreate[2]).fetchone() == 
	      logged_execute(cnx,'select count(*) from df_dtdict').fetchone()), '''
      Old and new df_dtdict creation methods disagree''';
    tprint("created df_dtdict",tt);tt = time.time()

    # rather than running the same complicated select statement multiple times 
    # for each rule in df_dtdict lets just run each selection criterion 
    # once and save it as a tag in the new RULE column
    # DONE: use df_rules
    # This is a possible place to use the new dsSel function (see below)
    #[logged_execute(cnx, ii[0]) for ii in logged_execute(cnx, par['dd_criteria']).fetchall()]
    #cnx.commit()
    dd_criteria = [dsSel(ii['rule'],ii['criterion'],"""
			 update df_dtdict set rule = '{0}'
			 where rule = 'UNKNOWN_DATA_ELEMENT' and 
			 """) for ii in ruledicts if ii['rule']!='UNKNOWN_DATA_ELEMENT']
    # Ah, okay. Figured out why ethnicity rule was being ignored. All the `rule` fields in
    # dd_dtdict start out as being UNKNOWN_DATA_ELEMENT, to mark them as not having been
    # matched by any preceding rule. The order in which the rules are tried matters-- the
    # more specific ones go in the beginning, and the more general ones toward the end, 
    # and the ultimate fallback rule targets the UNKNOWN_DATA_ELEMENT rows that remain.
    # So there was nothing wrong with the `ethnicity` rule (I think), but the `code` rule kept
    # jumping ahead of it in line (because ethnicity like a code, but we want it to be
    # handled in a special way, so it's a specific type of code). I originally thought this 
    # was due to dicts() inside the ruledicts list having an undefined ordering, and hence 
    # the OrderedDict branch. However, the dicts themselves aren't relied on for ordering...
    # it's the ordering of those dicts within the ruledicts list that matters, and lists 
    # should preserve ordering. And dd_criteria is also a list (of DDL statements) and those
    # also are in the right order. The entries in ruledicts and dd_criteria are in the right
    # order but for some reason subsection() duplicates them. Seems like it shouldn't cause
    # any wrong behavior and maybe not even a noticeable performance hit, but I guess my OCD
    # couldn't bear to let this happen, so I wrapped dd_criteria in set(). And THAT is what
    # scrambled their ordering! I am replaceing set() with a trick learned from StackExchange
    # Removing duplicates from dd_criteria
    # TODO: figure out why subsection returns duplicates in the first place
    # http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order
    dd_criteria = [dd_criteria[ii] for ii in range (0,len(dd_criteria)) if dd_criteria[ii] not in dd_criteria[:ii]]
    [logged_execute(cnx,ii) for ii in dd_criteria]
    cnx.commit()
    tprint("added rules to df_dtdict",tt);tt = time.time()
    
    """ create the create_dynsql table, which may make most of these 
    individually defined tables unnecessary. 
    See if the ugly code hiding behind par['create_dynsql'] can be replaced by 
    more concise dsSel Or maybe even if df_dynsql table itself can be replaced
    and we could do it all in one step
    
    DONE: use df_rules"""
    logged_execute(cnx, par['create_dynsql'])
    tprint("created df_dynsql table",tt);tt = time.time()
    #end_section dynsql
    
    ##section dynsql_experimental
    #'''not sure it's an improvement, but here is using the sqgr function nested in itself 
    #to create the equivalent of the df_dynsql table 
    #(note the kludgy replace and || stuff, needs to be done better)
    #the body of the query'''
    
    #foo = cnx.execute("select sqgr(lv,rv,lf,' ',rf,' ') from (select sub_slct_std||sqgr(trim(colcd)||trim(presuffix)||trim(suffix),'',replace(sub_payload,'ccode',0),'','','')||replace(sub_frm_std,'{cid}','''{0}''')||sbwr lf,colcd lv,replace(sub_grp_std,'jcode',0) rf,trim(colcd)||trim(presuffix) rv from df_rules join df_dtdict on trim(df_rules.rule) = trim(df_dtdict.rule) where concode=0 group by cid order by cid,grouping,subgrouping)").fetchall()
    ## or maybe even
    #foo1 = " ".join([ii[0] for ii in cnx.execute("select pyf(sub_slct_std||sqgr(tc(colcd,presuffix,suffix),'',replace(sub_payload,'ccode',0),'','','')||replace(sub_frm_std,'{cid}','''{0}''')||sbwr||replace(sub_grp_std,'jcode',1) ,colcd,tc(colcd,presuffix)) from df_rules join df_dtdict on trim(df_rules.rule) = trim(df_dtdict.rule) where concode=0 group by cid order by cid,grouping,subgrouping").fetchall()])
    ## doesn't currently work, but will when we replace the {} stuff permanently
    #"""
    #foo2 = " ".join([ii[0] for ii in cnx.execute("select pyf(sub_slct_std||sqgr(tc(colcd,presuffix,suffix),'',sub_payload,'','','')||sub_frm_std||sbwr||sub_grp_std,colcd,tc(colcd,presuffix)) from df_rules join df_dtdict on trim(df_rules.rule) = trim(df_dtdict.rule) where concode=0 group by cid order by cid,grouping,subgrouping").fetchall()])
    #"""
    ## the select part of the query
    #bar = cnx.execute("select group_concat(val) from (select distinct trim(colcd)||trim(presuffix)||trim(suffix) val from df_rules join df_dtdict on trim(df_rules.rule) = trim(df_dtdict.rule) where concode=0 order by cid,grouping,subgrouping)").fetchall()
    ## or maybe even
    #bar1=cnx.execute("select group_concat(val) from (select distinct tc(colcd,presuffix,suffix) val from df_rules join df_dtdict on trim(df_rules.rule) = trim(df_dtdict.rule) where concode=0 order by cid,grouping,subgrouping)").fetchall()[0][0]
    ## putting them together...
    #"select patient_num,start_date, "+bar[0][0]+" from df_joinme "+foo[0][0]
    ##end_section dynsql_experimental

    #section chunks_n_fulloutput2
    # each row in create_dynsql will correspond to one column in the output
    # here we break create_dynsql into more manageable chunks
    # again, if generated using dsSel, we might be able to manage those chunks script-side
    numjoins = logged_execute(cnx, "select count(distinct jcode) from df_dynsql").fetchone()[0]
    [logged_execute(cnx, par['chunk_dynsql'].format(ii,joffset)) for ii in range(0,numjoins,joffset)]
    cnx.commit();
    tprint("assigned chunks to df_dynsql",tt);tt = time.time()
    
    # code for creating all the temporary tables
    # where cmh.db slows down
    [logged_execute(cnx, ii[0]) for ii in logged_execute(cnx, par['maketables']).fetchall()]
    tprint("created all tables described by df_dynsql",tt);tt = time.time()
    
    # code for creating what will eventually replace the fulloutput table
    stage1 = logged_execute(cnx,par['fulloutput2']).fetchone()[0]
    logged_execute(cnx,stage1)
    tprint("created fulloutput2 table",tt);tt = time.time()
    #end_section chunks_n_fulloutput2 
    
    #section final_query
    # TODO: lots of variables being created here, therefore candidates for renaming
    # or refactoring to make simpler
    allsel = rdt('birth_date',dtcp) + """ birth_date, sex_cd 
    ,language_cd, race_cd, julianday(df_joinme.start_date) - julianday(""" + \
      rdt('birth_date',dtcp) + """) age_at_visit_days, julianday(""" + \
	rdt('death_date',dtcp) + ") - julianday(" + rdt('birth_date',dtcp) + ") age_at_death_days,"
    dynsqlsel = logged_execute(cnx, "select group_concat(colname) from df_dynsql").fetchone()[0]
    
    allqry = "create table if not exists fulloutput as select df_joinme.*," + allsel + dynsqlsel
    allqry += """ from df_joinme 
      left join patient_dimension pd on pd.patient_num = df_joinme.patient_num
      left join fulloutput2 fo on fo.patient_num = df_joinme.patient_num and fo.start_date = df_joinme.start_date
      """
    allqry += " order by patient_num, start_date"
    logged_execute(cnx, allqry)
    tprint("created fulloutput table",tt);tt = time.time()
    #end_section final_query
    
    #section colnames (dynames,stnames)
    # now we get the names for the universal (i.e. for every result) and
    # the number of query-specific columns
    ndycols = logged_execute(cnx, "select count(*) from df_dynsql").fetchone()[0]
    # note the ii[1], it's not a typo-- the first element of ii is its order in the table
    fonames = [ii[1] for ii in logged_execute(cnx,"pragma table_info(fulloutput)").fetchall()]
    # the last ndycols set of columns are the query stpecific, so they have dynamic names
    dynames = fonames[-ndycols:]
    # this means the rest are universal or have static names
    stnames = [ii for ii in fonames if ii not in dynames]
    # joining them into a single string to keep things simpler later
    #end_section colnames
    
    #section binoutput_optional
    # We create a view of the above that collapses the aggregated code columns to 
    # binary T/F values
    selbin_dynsql = logged_execute(cnx, par['selbin_dynsql']).fetchone()[0]
    binoutqry = """create view df_binoutput as select """+",".join(stnames)
    binoutqry += ","+selbin_dynsql
    #binoutqry += ","+",".join([ii[1] for ii in logged_execute(cnx, "pragma table_info(loincfacts)").fetchall()[2:]])
    binoutqry += " from fulloutput"
    logged_execute(cnx, "drop view if exists df_binoutput")
    logged_execute(cnx, binoutqry)
    tprint("created df_binoutput view",tt);tt = time.time()

    if style == 'simple':
      finalview = 'df_binoutput'
    else:
      finalview = 'fulloutput'
    #end_section binoutput_optional
    
    #section remove_empty_cols (dycnts)
    # Okay, so this is the reason we don't have a .csv output bloated with empty columns
    # even before we started filtering the df_codeid table in this patch. However, this patch
    # should make the code run faster and the output .db smaller
    # do value-counts for each column
    # we have to do a naked cursor here because we'll need to get 
    # column names from it
    dycnts = logged_execute(
      cnx,"select "+", ".join(
	["count(case when {0} not in ('','@',' ','F') then 1 end)".format(ii) for ii in dynames]
	)+" from fulloutput"
      ).fetchone()
    # here are the columns we keep
    # TODO: write dyncnts to df_dynsql table
    #end_section remove_empty_cols

    # COLUMN NAMES to keep
    keepdynames = [ii[0] for ii in zip(dynames,dycnts) if ii[1] > mincnt]
    
    # values for the first row of output
    #section outputmeta
    # Static columns without JSON 
    
    # use '---' if not matched by either of the below two
    outputmeta = ['---' if kk in stnames else kk 
		  # if not date, THEN use 0 for numeric columns
		  for kk in [0 if '_days' in jj else jj 
	       # FIRST use current date for date columns
	       for jj in [time.strftime('%Y-%m-%d',time.gmtime()) if '_date' in ii else ii 
		   for ii in stnames]]]
    
    # Field names for the JSON (non-static) columns
    # adding ccd_list to hold the distinct values of concept_cds for each column
    jfields = [ii[1] for ii in logged_execute(cnx,"pragma table_info(df_dtdict)").fetchall()]+['ccd_list']
    # generate a dict for each (dynamic) variable using jfields as the keys 
    # and the rows of df_dtdict as values them to JSON strings, and extend()
    # this list onto outputmeta
    outputmetaqry='''
    select df_dtdict.*,ccd_list from df_dtdict left join
    (select id,group_concat(distinct ccd) ccd_list
    from df_codeid where id in 
    (select cid from df_dtdict where ccd <= {0})
    group by id) ccdlist on cid = id'''.format(par['dtdict_ccd_cutoff']);
    outputmeta.extend([json.dumps(yy)
	     for yy in [dict(zip(jfields,xx)) 
		 for xx in logged_execute(cnx,outputmetaqry).fetchall()] 
	     if yy['colid'] in keepdynames]);
    #vcrs = cnx.execute(
    #  "select "+",".join(
	 #[ii[0] for ii in cnx.execute(
	 #"select ' count( '||colname||') '||colname from df_dynsql"
	 #).fetchall()])+" from fulloutput")
    # get column names
    # vnms = [ii[0] for ii in vcrs.description]
    #end_section outputmeta
    
    # i.e. to not create a .csv file, pass 'none' in the -v argument
    if fname.lower() != 'none':
      #section first2lines
      ff = open(fname,'wb')
      finalnames = stnames + keepdynames
      # below line generates the CSV header row
      csv.writer(ff).writerow(finalnames)
      csv.writer(ff).writerow(outputmeta)
      #end_section first2lines
      
      #section rest_of_output
      # fetch the final data 
      result = logged_execute(
	cnx, "select {0} from {1}".format(
	  ",".join(finalnames),finalview
	  )
	).fetchall()
      # write the data to csv
      with ff:
	  csv.writer(ff).writerows(result)
      tprint("wrote output table to file",tt);tt = time.time()
      #end_section rest_of_output

      # now the metadata
      #section write_metadata
      path = dirname(fname)
      if path == '': path = '.'
      f0 = open(path + '/meta_'+basename(fname),'wb')
      #import pdb; pdb.set_trace()
      csv.writer(f0).writerow(cols_meta)
      result = logged_execute(cnx,'select '+','.join(cols_meta)+' from df_dynsql').fetchall()
      with f0: csv.writer(f0).writerows(result)
      tprint("wrote metadata to file",tt);tt = time.time()
      #end_section write_metadata
      
    tprint("TOTAL RUNTIME",startt)
    
    """ notes
    DONE: implement a user-configurable 'rulebook' containing patterns for catching data that would otherwise fall 
    into UNKNOWN FALLBACK, and expressing in a parseable form what to do when each rule is triggered.
    DONE: The data dictionary will contain information about which built-in or user-configured rule applies for each cid
    We are probably looking at several different 'dcat' style tables, broken up by type of data
    DONE: We will iterate through the data dictionary, joining new columns to the result according to the applicable rule
    """

def db2df(db,csvfile=None,fileclean=True,mincnt=0,returnwhat='file'
	  ,buffering=-1
):
  '''
  db: name of sqlite db file or sqlite3 connection
  csvfile: name of output file (not connection)
  fileclean: if True (default) will remove df-created tables before running
  mincnt: if fewer records than this number, drop the column
  returnwhat: either 'file' or 'str' (the latter being the file name)
  '''
  tablesneeded=set(('concept_dimension','patient_dimension','variable'
		    ,'observation_fact'))
  tablesquery='select name from sqlite_master where type = "table"'
  
  # make sure there is a valid sqlite connection
  if type(db) in (str,unicode): con = sq.connect(db)
  elif type(db) == sq.Connection: con = db
  else: 
    raise NotImplementedError('''
      db2df: the value passed to the 'db' argument is type '%s' but at 
      this time only 'str','unicode', or 'sqlite3.Connection' types are
      supported.''' % type(db))
  # make sure there is a name for the output file
  if not csvfile:
    if type(db) in (str,unicode): csvfile = db.replace('.db','')+'.csv'
    else:
      try: csvfile = basename(con.execute('''
	PRAGMA database_list''').fetchall()[0][2]).replace('.db','')+'.csv'
      except: csvfile = mkstemp(suffix='.csv')
      
  # (try to) make sure the db is valid databuilder output
  assert len(tablesneeded & set(xx[0].lower()\
    for xx in con.execute(tablesquery).fetchall()))==4,'''
  db2df: the 'db' argument, '%s' is an sqlite database but is not a valid
  databuilder file because it is missing one or more of the following tables:
  %s''' % (db,','.join(tablesneeded))
  # remove any previous datafinisher stuff left there
  if fileclean: cleanup(con)
  # create csv output
  main(con,csvfile,style='concat',dtcp=CONSOLE_ARGS.datecompress,mincnt=float(mincnt))
  # cleanup
  con.close()
  if returnwhat == 'file': return(open(csvfile,mode='r',buffering=buffering))
  else: return csvfile 
    
	
if __name__ == '__main__':
  dolog = CONSOLE_ARGS.log

  con = sq.connect(CONSOLE_ARGS.dbfile)

  if CONSOLE_ARGS.csvfile == 'OUTFILE':
    csvfile = CONSOLE_ARGS.dbfile.replace(".db","")+".csv"
  else:
    csvfile = CONSOLE_ARGS.csvfile
    
  # OVERWRITE suggestions
  if CONSOLE_ARGS.suggest:
    suggestor=json.loads(''.join([xx for xx in\
      open(CONSOLE_ARGS.suggest).read().split('\n') if not xx.startswith('#')]))
  else: suggestor=autosuggestor
  
  # but MERGE IN rules
  runrules=deepcopy(rules)
  if CONSOLE_ARGS.rules:
    runrules.update(json.loads(''.join([xx for xx in\
      open(CONSOLE_ARGS.rules).read().split('\n') if not xx.startswith('#')])))

  
  if CONSOLE_ARGS.cleanup:
    cleanup(con)
  else:
    DFMeta(CONSOLE_ARGS.dbfile,suggestions=suggestor).processRows(outfile=csvfile)
    #main(con,csvfile,args.style,dtcp,float(args.minimumcount))



