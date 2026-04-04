#!/usr/bin/env python3
"""Compare two pytest-benchmark JSON files and print a delta table.

Usage: bench_compare_py.py OLD.json NEW.json
"""
import json
import sys


def load(path: str) -> dict[str, float]:
    with open(path) as f:
        data = json.load(f)
    return {b["name"]: b["stats"]["mean"] for b in data["benchmarks"]}


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} OLD.json NEW.json", file=sys.stderr)
        sys.exit(1)

    old = load(sys.argv[1])
    new = load(sys.argv[2])
    all_names = sorted(set(old) | set(new))

    W = 62
    print(f"{'Benchmark':<{W}} {'old mean':>12} {'new mean':>12} {'delta':>10}")
    print("-" * (W + 38))
    for name in all_names:
        o, v = old.get(name), new.get(name)
        if o and v:
            delta = (v - o) / o * 100
            print(f"{name:<{W}} {o*1e3:>11.3f}ms {v*1e3:>11.3f}ms {delta:>+9.1f}%")
        elif v:
            print(f"{name:<{W}} {'(new)':>12} {v*1e3:>11.3f}ms {'':>10}")
        else:
            print(f"{name:<{W}} {o*1e3:>11.3f}ms {'(gone)':>12} {'':>10}")


if __name__ == "__main__":
    main()
