'''
This is a collection of python dict objects telling the xmetaj() function what 
rules and titles to suggest under what circumstances

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
    ,"criteria":"ccd <= 1" # later, check for > 1 unique concept|mod per visit
    ,"extractors":[["true_false","{0}.tf"]]
   }

  ,{ # if this column has codes (and really anything else)
     "name": "code_concat"
    ,"criteria":"True"
    ,"extractors":[["concat_unique","{0}.values"]]
   }

]
