'''
This is a collection of python dict objects telling the xmetaj() function what 
rules and titles to suggest under what circumstances

The criteria will be executed by eval() and for each of the 2-items lists inside 
the extractor list the first value will the extractor name and the second will be
formatted to be the column name
'''
rules = [
   { # if this column has any numeric values return the last for each visit
     "name": "last_numeric"
    ,"criteria":"nval_num > 0"
    ,"extractors":[["last_numeric","{0}.num"]]
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
