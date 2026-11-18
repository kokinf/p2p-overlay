"""Check marker confidentiality and positive delivery evidence during capture."""
import argparse, datetime, json, re, struct
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--pcap',required=True);p.add_argument('--logs',required=True);p.add_argument('--marker',default='pcap-control-secret-17nov2026');p.add_argument('--output',required=True);a=p.parse_args()
b=Path(a.pcap).read_bytes()
if len(b)<24:raise SystemExit('Invalid or empty pcap')
endian='<' if b[:4]==bytes.fromhex('d4c3b2a1') else '>';off=24;stamps=[]
while off+16<=len(b):
 sec,micro,size,_=struct.unpack_from(endian+'IIII',b,off)
 if off+16+size>len(b):raise SystemExit('Truncated pcap')
 stamps.append(sec+micro/1e6);off+=16+size
if not stamps:raise SystemExit('No packets')
deliveries=[]
for line in Path(a.logs).read_text(encoding='utf-8',errors='replace').splitlines():
 if a.marker not in line or '[APP]' not in line:continue
 m=re.search(r'\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',line)
 if m:
  stamp=datetime.datetime.strptime(m.group(),'%Y/%m/%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc).timestamp()
  if min(stamps)<=stamp<=max(stamps):deliveries.append(line)
r={'packets':len(stamps),'bytes':len(b),'plaintext_present':a.marker.encode() in b,'deliveries_during_capture':len(deliveries),'capture_start_utc':datetime.datetime.fromtimestamp(min(stamps),datetime.timezone.utc).isoformat(),'capture_end_utc':datetime.datetime.fromtimestamp(max(stamps),datetime.timezone.utc).isoformat()}
Path(a.output).write_text(json.dumps(r,indent=2));print(json.dumps(r));raise SystemExit(0 if not r['plaintext_present'] and deliveries else 1)
