import os
import json
import glob
import argparse

def check_metrics(directory, N, K):
    files = glob.glob(os.path.join(directory, "metrics-*.json"))
    if not files:
        print("[FAIL] No metrics files found!")
        return False

    total_nodes = len(files)
    if total_nodes != N:
        print("[FAIL] Expected metrics from all N nodes")
        return False
    nodes_with_full_table = 0
    total_lookups = 0
    successful_multihop_lookups = 0
    initial_hits = 0

    print(f"Analyzing {total_nodes} metrics files...")

    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)

        contacts = [c for b in data['buckets'] for c in b['contacts']]
        if any(len(b['contacts']) > K for b in data['buckets']) or len(set(contacts)) != len(contacts) or data['node_id'] in contacts:
            print('[FAIL] Invalid routing table')
            return False
        contacts_count = len(contacts)
        
        if contacts_count >= (N - 1):
            nodes_with_full_table += 1

        for lookup in data.get('lookups') or []:
            if isinstance(lookup['target'], list):
                lookup['target'] = bytes(lookup['target']).hex()
            total_lookups += 1
            
            if lookup['target'] not in (lookup.get('initial_closest') or []):
                initial_hits += 1
                
                if lookup['iterations'] >= 2 and lookup['target'] in (lookup.get('closest') or []):
                    successful_multihop_lookups += 1

    print("\n - Non-Degeneracy -")
    pct_full = (nodes_with_full_table / total_nodes) * 100
    print(f"Nodes with full table (>= N-1 contacts): {nodes_with_full_table}/{total_nodes} ({pct_full:.1f}%)")
    
    if pct_full > 20.0:
        print("[FAIL] Too many nodes have a full table. DHT is degenerate.")
        return False
    print("[PASS] Routing table occupancy check passed (partial criterion).")

    print("\n - Lookups -")
    print(f"Total lookups: {total_lookups}")
    print(f"Lookups where target was NOT in initial_closest: {initial_hits}")
    print(f"Successful multi-hop lookups (iterations >= 2): {successful_multihop_lookups}")

    if successful_multihop_lookups < 30:
        print(f"[FAIL] Need at least 30 multi-hop lookups, got {successful_multihop_lookups}")
        return False
    
    print("[PASS] Lookup requirements met.")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="Directory with metrics-*.json")
    parser.add_argument("--n", type=int, required=True, help="Network size N")
    parser.add_argument("--k", type=int, required=True, help="K_BUCKET_SIZE")
    args = parser.parse_args()

    if not check_metrics(args.dir, args.n, args.k):
        exit(1)