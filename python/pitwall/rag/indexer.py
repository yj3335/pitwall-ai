"""Index the FIA regulations corpus into Qdrant.

    python -m pitwall.rag.indexer [--recreate]
"""
from __future__ import annotations

import argparse
import json

from pitwall import config
from pitwall.rag.store import HybridRetriever, load_corpus


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recreate", action="store_true", default=True)
    ap.add_argument("--collection", default=config.QDRANT_COLLECTION)
    args = ap.parse_args()
    docs = load_corpus()
    print(f"indexing {len(docs)} chunks into {config.QDRANT_URL}/{args.collection} with {config.EMBED_MODEL}")
    r = HybridRetriever(collection=args.collection)
    stats = r.index(docs, recreate=args.recreate)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
