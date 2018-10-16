#!/usr/bin/env python3

import hashlib
from datetime import datetime
from dateutil import parser

def do_transform(value, trans_type):
  try:
      # Transforms any input to NULL
      if trans_type == "SET-NULL":
          return None
      # Transfroms string input to hash
      elif trans_type == "HASH":
          return hashlib.sha256(value.encode('utf-8')).hexdigest()
      # Transforms string input to hash skipping first n characters, e.g. HASH-SKIP-FIRST-2
      elif 'HASH-SKIP-FIRST' in trans_type:
          return value[:int(trans_type[-1])] + hashlib.sha256(value.encode('utf-8')[int(trans_type[-1]):]).hexdigest()
      # Transforms any date to stg
      elif trans_type == "MASK-DATE":
          return parser.parse(value).replace(month=1, day=1).isoformat()
      # Transforms any number to zero
      elif trans_type == "MASK-NUMBER":
          return 0
      # Return the original value if cannot find transformation type
      else:
          return value
  
  # Return the original value if cannot transform
  except Exception:
      return value
    
