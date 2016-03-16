## Table Names 
This is a brief note on the renaming of tables that we did in commit d45eb4958298716a75975f0a9e54deaa16ee0e47 but neglected to document the mapping of old names to new names. Here they are:

| Old Name | New Name |
|----------|----------|
|SCAFFOLD  |DF_JOINME |
|OBS_DF    |DF_OBSFACT|
|DD2       |DF_DYNSQL |
|DATA_DICTIONARY|DF_DTDICT|
|RULEDEFS  |DF_RULES  |
|FULLOUTPUT2|FULLOUTPUT2|
|FULLOUTPUT|FULLOUTPUT|

## TODO: Thoughts on naming variables that hold SQL snippets...
* For table creation standardize on a cr_ prefix a dedicated section
* For SQL that returns results, do sel_ or dedicated section
* For SQL that updates, do up_ or dedicated section


## What each table does

### Before we start talking about what each table does, we need to define an idea that will get used repeatedly here: variable-id. 
In the DataBuilder UI, the user drags folder or leaf nodes from the i2b2 concept tree to the "Other Observations" field. What got dragged there is preserved in the .db file within the VARIABLE table. Each is assigned an ID value in that table. Let's call these variable-ids. These IDs allow us to programmatically group concepts from OBSERVATION_FACT instead of blindly and uselessly creating a separate column for each and every distinct concept. At the moment, concepts sharing the same variable-id get grouped into a single main indicator column (on this visit, did the patient have one or more of ANY of these facts) for concepts that are codes (such as diagnoses, allergies, family history, procedures, and orders). Concepts that share the same variable-id and have values (such as vitals and labs) do explode out into as many columns as there are distinct concept codes, but all these columns are adjacent to each other and share the same prefixes (based on their variable-id). *BY DESIGN REDUNDANT MEMBERSHIP OF THE SAME CONCEPT IN MULTIPLE VARIABLE-IDs IS PERMITTED* .

**TODO: move someplace more appropriate (maybe at creation) the below reasons for each of the following tables' existence:**

### df_codeid
A mapping of concept_cd (ccd) to variable ID (id) to data domain (ddomain) to a standard code (ICD9 or LOINC) and if there isn't one then a concept path (cpath).

### df_obsfact
Modified version of observation fact. Created from OBSERVATION_FACT and DF_CODEID. It's basically a slightly cleaned-up version of CONCEPT_DIMENSION, with non-informative or meaningless entries omitted (including the bogus demographic code entries that are redundant with PATIENT_DIMENSION). An important thing that happens here is that START_DATE gets replaced with JULIAN_DAY(START_DATE) rounded to a user-specified number of days (via the -d argument) and converted back to a date using the RDST() or RDT() function we register as a UDF in SQLite

### df_rules
rule definitions. Needs a documentation effort all its own, coming soon to a branch near you

### df_dtdict
Created by sql/dd.sql out of the df_codeid, variable and df_obsfact tables. df_codeid provides the data domains and counts of distinct CONCEPT_CDs, and df_obsfact provides counts of various of its data fields, both available to be used by the rules in df_rules. df_dtdict contains a ton of information, grouped by which elements were dragged over as one folder to the concepts field in the DataBuilder query interface. These all get the same ID. DDOMAIN comes from df_codeid as per above. CCDs is the number of distinct CONCEPT_CDs for this CID.  MXINSTS, MXFACTS, are currently duplicates of each other (bug). I _believe_ mxinsts represents the maximum number of distinct entries of any variable-id/patient/start-date combination in OBS_DF (gives some idea of how many modifiers or instances of the same class of fact for the same patient on the same visit there are). COLCD is pasted together out of 'v'and a 0-padded CID, COLID is similarly put together but also has a semi-human-readable suffix created by abbreviating the hell out of VARIABLE.NAME, NAME is from there too. CONCEPT_PATH is from VARIABLE.CONCEPT_PATH ... MOD,TVAL_CHAR,VALUEFLAG_CD,UNITS_CD,CONFIDENCE_NUM,QUANTITY_NUM,LOCATION_CD: these are all number of unique values (with NULLs and certain non-informative values excluded) in the corresponding columns of OBS_DF. NVAL_NUM becomes an indicator variable reporting whether ANY of the facts for a given variable-id in OBS_DF have a numeric value in their NVAL_NUM field (otherwise it is NULL in this table). Finally, VALTYPE_CD is a concatenated list of all distinct VALTYPE_CD values for that variable-id in OBS_DF.

### df_dynsql
Created from df_rules, df_dtdict, and DF_CODEID. This table is mostly a storage place for snippets of SQL that get concatenated together to make dynamic SQL statements for creating the output (including chunking it into multiple dynamically named tables, whose names are recorded in the TTABLE column by the code in chunkdf_dynsql). The reason for chunking is that there is a limit to how many joins can be done in one SQLite statement. So, we perform a safe number of joins creating several tables named tXXX, and then we join those tables. This is a concise and robust alternative to looping through all this shit within Python.

### datafinisher_log
Persistent log (deliberately no df_ in the name)

### df_vars
Persistent variable storage (stores only one variable right now)

### df_joinme (formerly called SCAFFOLD)
All unique combinations of PATIENT_NUM and START_DATE with the latter converted using JULIAN_DAY() and rounded to a user-specified number of days (via the -d argument) and converted back to a date using the RDST() or RDT() function we register as a UDF in SQLite. The point of this table, and the reason it's named df_joinme is that it represents an OR of every distinct patient-visit in the data, to which everything can later be joined without errors.

### fulloutput2
Should rename to df_preoutput. Created from df_joinme, the dynamic SQL generated using df_dynsql, and the tXXX tables (self joined on a whole lot of concepts from OBS_DF) also generated using DD2. This is what results from joining the tXXX tables as described in the description of df_dynsql above 

### fulloutput
Should rename to df_fulloutput. Created from SCAFFOLD,PATIENT_DIMENSION,the dynamic SQL generated using DD2, and FULLOUTPUT2
This is the actual final result from df.py with the default -s option. If -s is set to 'simple' the result will be from a view of the FULLOUTPUT table that replaces concatenated codes are replaced with True/False values. This is basically FULLOUTPUT2 but with data from PATIENT_DIMENSION joined on, and sorted by PATIENT_NUM and START_DATE (that have in a previous step passed through RDST())
