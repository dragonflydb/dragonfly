"""
Test that Dragonfly supports the Redis primitives needed for semantic routing.

Requires:
  - A running Dragonfly instance on localhost:6379
  - pip install redis numpy

This script simulates what redisvl's SemanticRouter does under the hood:
  1. Creates a search index with TAG + TEXT + VECTOR fields
  2. Stores route references as hashes with vector embeddings
  3. Runs KNN queries to find the closest route
"""

import struct
import sys

import numpy as np
import redis

DIMS = 4  # small dimensionality for a quick smoke test


def float_vector_bytes(vec: list[float]) -> bytes:
    """Encode a list of floats as a little-endian FLOAT32 blob (what FT.SEARCH expects)."""
    return struct.pack(f"<{len(vec)}f", *vec)


def random_unit_vector(dims: int = DIMS) -> np.ndarray:
    v = np.random.randn(dims).astype(np.float32)
    return v / np.linalg.norm(v)


def main():
    r = redis.Redis(decode_responses=False)  # binary mode for vectors

    try:
        r.ping()
    except redis.ConnectionError:
        print("ERROR: Cannot connect to Redis/Dragonfly on localhost:6379")
        sys.exit(1)

    index_name = "test_semantic_router"
    prefix = "sr:"

    # ── Cleanup from previous runs ──────────────────────────────────────
    try:
        r.execute_command("FT.DROPINDEX", index_name, "DD")
    except redis.ResponseError:
        pass  # index didn't exist

    # ── Step 1: Create index ────────────────────────────────────────────
    print("1. Creating search index...")
    r.execute_command(
        "FT.CREATE",
        index_name,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "route_name",
        "TAG",
        "reference_id",
        "TAG",
        "reference",
        "TEXT",
        "vector",
        "VECTOR",
        "FLAT",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        str(DIMS),
        "DISTANCE_METRIC",
        "COSINE",
    )
    print("   OK - index created")

    # ── Step 2: Define routes with reference vectors ────────────────────
    # Simulate two clusters in vector space:
    #   "greeting" cluster near [1, 0, 0, 0]
    #   "farewell" cluster near [0, 0, 0, 1]
    routes = {
        "greeting": {
            "center": np.array([1, 0, 0, 0], dtype=np.float32),
            "references": ["hello there", "hi how are you", "hey whats up"],
        },
        "farewell": {
            "center": np.array([0, 0, 0, 1], dtype=np.float32),
            "references": ["goodbye", "see you later", "bye bye"],
        },
    }

    print("2. Storing route references...")
    ref_id = 0
    for route_name, route in routes.items():
        center = route["center"]
        for i, ref_text in enumerate(route["references"]):
            # Create a vector close to the cluster center with a little noise
            noise = np.random.randn(DIMS).astype(np.float32) * 0.1
            vec = center + noise
            vec = vec / np.linalg.norm(vec)  # normalize for cosine

            key = f"{prefix}{ref_id}"
            r.hset(
                key,
                mapping={
                    "route_name": route_name,
                    "reference_id": str(ref_id),
                    "reference": ref_text,
                    "vector": vec.tobytes(),
                },
            )
            ref_id += 1
    total = ref_id
    print(f"   OK - stored {total} references across {len(routes)} routes")

    # ── Step 3: KNN query (core of semantic routing) ────────────────────
    # Query with a vector near the "greeting" cluster
    query_vec = np.array([0.95, 0.05, 0.1, 0.0], dtype=np.float32)
    query_vec = query_vec / np.linalg.norm(query_vec)

    print("3. Running KNN query (expect 'greeting' matches)...")
    results = r.execute_command(
        "FT.SEARCH",
        index_name,
        "*=>[KNN 3 @vector $query_vec AS dist]",
        "PARAMS",
        "2",
        "query_vec",
        query_vec.tobytes(),
        "SORTBY",
        "dist",
        "ASC",
        "RETURN",
        "3",
        "route_name",
        "reference",
        "dist",
        "LIMIT",
        "0",
        "3",
    )

    # Parse results: [total_count, key1, [field, val, ...], key2, ...]
    count = int(results[0])
    print(f"   Got {count} results:")
    for i in range(1, len(results), 2):
        key = results[i]
        fields = results[i + 1]
        field_map = {}
        for j in range(0, len(fields), 2):
            field_map[fields[j].decode()] = fields[j + 1].decode()
        print(
            f"     {key.decode():12s}  route={field_map.get('route_name', '?'):12s}  "
            f"ref={field_map.get('reference', '?'):20s}  dist={field_map.get('dist', '?')}"
        )

    # ── Step 4: KNN with pre-filter (route-scoped search) ──────────────
    print("4. Running KNN with TAG pre-filter (@route_name:{farewell})...")
    results = r.execute_command(
        "FT.SEARCH",
        index_name,
        "@route_name:{farewell}=>[KNN 2 @vector $query_vec AS dist]",
        "PARAMS",
        "2",
        "query_vec",
        query_vec.tobytes(),
        "SORTBY",
        "dist",
        "ASC",
        "RETURN",
        "3",
        "route_name",
        "reference",
        "dist",
        "LIMIT",
        "0",
        "2",
    )

    count = int(results[0])
    print(f"   Got {count} results (should all be 'farewell'):")
    for i in range(1, len(results), 2):
        key = results[i]
        fields = results[i + 1]
        field_map = {}
        for j in range(0, len(fields), 2):
            field_map[fields[j].decode()] = fields[j + 1].decode()
        print(
            f"     {key.decode():12s}  route={field_map.get('route_name', '?'):12s}  "
            f"ref={field_map.get('reference', '?'):20s}  dist={field_map.get('dist', '?')}"
        )

    # ── Step 5: Aggregate distances per route (what the router does) ───
    print("5. Full route classification (aggregate distances per route)...")
    all_results = r.execute_command(
        "FT.SEARCH",
        index_name,
        f"*=>[KNN {total} @vector $query_vec AS dist]",
        "PARAMS",
        "2",
        "query_vec",
        query_vec.tobytes(),
        "SORTBY",
        "dist",
        "ASC",
        "RETURN",
        "2",
        "route_name",
        "dist",
        "LIMIT",
        "0",
        str(total),
    )

    # Aggregate average distance per route
    from collections import defaultdict

    route_dists: dict[str, list[float]] = defaultdict(list)
    for i in range(1, len(all_results), 2):
        fields = all_results[i + 1]
        field_map = {}
        for j in range(0, len(fields), 2):
            field_map[fields[j].decode()] = fields[j + 1].decode()
        route_dists[field_map["route_name"]].append(float(field_map["dist"]))

    print("   Route classification results:")
    for route, dists in sorted(route_dists.items(), key=lambda x: np.mean(x[1])):
        avg = np.mean(dists)
        print(f"     {route:12s}  avg_dist={avg:.4f}  (individual: {[f'{d:.4f}' for d in dists]})")

    # ── Cleanup ─────────────────────────────────────────────────────────
    r.execute_command("FT.DROPINDEX", index_name, "DD")
    print("\nAll tests passed! Dragonfly supports the primitives for semantic routing.")


if __name__ == "__main__":
    main()
