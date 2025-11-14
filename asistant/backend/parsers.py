import re
from datetime import datetime

LOG_RE = re.compile(
    r"(?P<ts_meas>\d{4}\.\s*\d{2}\.\s*\d{2}\.\s*\d{2}시\s*\d{2}분\s*\d{2}초)\s*\|\s*기록:\s*(?P<ts_log>[\d\.\s:시분초]+)\s*"
    r"컬럼:\s*(?P<param_name>.+?)\s*\((?P<param_id>[^)]+)\)\s*"
    r"상태=(?P<state>\w+),\s*세트=(?P<setpoint>[-\d\.]+),\s*위반지속=(?P<viol_sec>[\d\.]+)s\s*::\s*"
    r".*?값\s*(?P<value>[-\d\.]+),\s*기준\s*(?P<baseline>[-\d\.]+),\s*편차\s*(?P<dev_pct>[-\d\.]+)%,\s*허용±(?P<tol_pct>[-\d\.]+)%"
)

def parse_log(line:str)->dict:
    m = LOG_RE.search(line)
    if not m:
        return {"raw": line, "parsed": False}
    d = m.groupdict()
    d["parsed"] = True
    d["viol_sec"] = float(d["viol_sec"])
    for k in ["setpoint","value","baseline","dev_pct","tol_pct"]:
        d[k] = float(d[k])
    # 간단한 시간 정규화(원본 보존)
    d["ts_meas_iso"] = d["ts_meas"]
    d["ts_log_iso"]  = d["ts_log"]
    return d
