myStr = """
    job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }

job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }

job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }
"""

myStr2 = """
    job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }
"""

def myfilter():
    result = myStr.split("job_stats:")
    del result[0]
    for data in result:
        print("This is split data-->")
        for line in data.splitlines():
            if not line.strip():
                continue
            print(line)

def mylist():
    for line in myStr.splitlines():
        attr = line.split(':')[0].strip()
        if "_bytes" in attr:
            attr_ext = {"" : 2, "_min" :4 , "_max" : 5, "_sum" : 6}
            for ext in attr_ext:
                inx = attr_ext[ext]
                objattr = attr + ext
                delim2 = '}' if  inx == 6 else ','
                value = line.split(':')[inx].split(delim2)[0].strip()
                print(objattr + " = " + value)

mylist()
