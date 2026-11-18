import argparse, csv, json, re, statistics
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('--dir',type=Path,required=True); p.add_argument('--repeats',type=int,default=5); a=p.parse_args()
rows=[]
for line in (a.dir/'raw.jsonl').read_text(encoding='utf-8-sig').splitlines():
 e=json.loads(line)
 if e.get('Action')=='fail': raise SystemExit('Failed test in raw results')
 m=re.search(r'FILE_METRIC bytes=(\d+) seconds=([\d.]+) goodput_Bps=([\d.]+) sha256=([0-9a-f]{64})',e.get('Output',''))
 if m: rows.append(dict(bytes=int(m[1]),seconds=float(m[2]),goodput_Bps=float(m[3]),sha256=m[4]))
summary=[]
for size in (1024,102400,1048576):
 sample=[r for r in rows if r['bytes']==size]
 if len(sample)!=a.repeats: raise SystemExit(f'Expected {a.repeats} measurements for {size}, got {len(sample)}')
 values=[r['goodput_Bps'] for r in sample]
 summary.append(dict(bytes=size,n=len(values),min_Bps=min(values),max_Bps=max(values),median_Bps=statistics.median(values),mean_Bps=statistics.mean(values)))
for name,data in [('transfers.csv',rows),('summary.csv',summary)]:
 with (a.dir/name).open('w',newline='',encoding='utf-8') as f:
  w=csv.DictWriter(f,fieldnames=list(data[0]));w.writeheader();w.writerows(data)
print(f'PASS: {len(rows)} SHA-256 checked transfers; transfers.csv and summary.csv saved')
