

def fix_region_id(rid):
    rid = str(rid)
    if len(rid) == 7:
        rid = "0" + rid  # now 8 chars
    return rid[:-3]     # remove last 3 chars