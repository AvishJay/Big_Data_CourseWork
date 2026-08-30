
import argparse
import gzip
import os
import sys


def open_any(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "r")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source", help="cit-Patents.txt or cit-Patents.txt.gz")
    ap.add_argument("-n", "--num-edges", type=int, default=5000)
    ap.add_argument("-o", "--out", default="import/citations.csv")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    written = 0
    nodes = set()

    with open_any(args.source) as src, open(args.out, "w") as out:
        out.write("citing,cited\n")                    
        for line in src:
            if line.startswith("#") or not line.strip():
                continue                              
            parts = line.split()
            if len(parts) != 2:
                continue
            citing, cited = parts
            out.write(f"{citing},{cited}\n")
            nodes.update((citing, cited))
            written += 1
            if written >= args.num_edges:
                break

    if written < args.num_edges:
        print(f"[warn] source only contained {written} edges", file=sys.stderr)
    print(f"[done] {written} citation edges ({len(nodes)} distinct patents) "
          f"-> {args.out}")
    print("Next: docker compose up -d, then run cypher/01_import.cypher")


if __name__ == "__main__":
    main()
