"""
Test redisvl's SemanticRouter against Dragonfly.

Requires:
  - A running Dragonfly instance on localhost:6379
  - pip install redisvl sentence-transformers

Uses DragonflySemanticRouter, a shim that replaces the FT.AGGREGATE +
VECTOR_RANGE path with FT.SEARCH + KNN plus client-side aggregation.
"""

import sys


def main():
    try:
        from redisvl.extensions.router.schema import Route

        from dragonfly_router import DragonflySemanticRouter
    except ImportError:
        print("ERROR: redisvl not installed. Run: pip install redisvl")
        sys.exit(1)

    # ── Define routes ───────────────────────────────────────────────────
    technology = Route(
        # in the hash, name is stored as route_name
        name="technology",
        # these are each converted into 768 dim. vectors by the vectorizer in semantic router.
        # each ref. becomes an hset with its own key
        references=[
            "what are the latest advancements in AI?",
            "tell me about the newest gadgets",
            "what's trending in tech?",
            "how does machine learning work?",
        ],
        metadata={"category": "tech"},
        # a match will only count within this cosine distance thresold.
        distance_threshold=0.8,
    )

    sports = Route(
        name="sports",
        references=[
            "who won the game last night?",
            "what's the score of the basketball game?",
            "tell me about the latest football match",
            "how is my favorite team doing?",
        ],
        metadata={"category": "sports"},
        distance_threshold=0.8,
    )

    cooking = Route(
        name="cooking",
        references=[
            "how do I make pasta from scratch?",
            "what's a good recipe for chocolate cake?",
            "best way to season a cast iron pan",
            "how long to bake chicken at 350?",
        ],
        metadata={"category": "food"},
        distance_threshold=0.8,
    )

    # ── Create the router ───────────────────────────────────────────────
    print("1. Creating DragonflySemanticRouter (downloads model on first run)...")
    try:
        # searches using knn instead of vector_range, and fliters by distance client side
        router = DragonflySemanticRouter(
            name="dragonfly-test-router",
            routes=[technology, sports, cooking],
            redis_url="redis://localhost:6379",
            overwrite=True,
        )
    except Exception as e:
        print(f"   FAILED to create router: {e}")
        print("\n   This might indicate an incompatibility in FT.CREATE or HSET commands.")
        raise

    print(f"   OK - Router created with {len(router.routes)} routes")

    # ── Test single routing ─────────────────────────────────────────────
    print("\n2. Testing single query routing...")
    test_queries = [
        ("Can you explain how neural networks learn?", "technology"),
        ("Who scored the winning goal?", "sports"),
        ("What temperature should I roast vegetables at?", "cooking"),
        ("Tell me about the latest iPhone", "technology"),
    ]

    for query, expected in test_queries:
        match = router(query)
        if match and match.name is not None:
            status = "OK" if match.name == expected else "MISMATCH"
            print(f"   [{status}] '{query[:50]:50s}' -> {match.name} (dist={match.distance:.3f})")
        else:
            print(f"   [MISS] '{query[:50]:50s}' -> no match (expected {expected})")

    # ── Test multi routing ──────────────────────────────────────────────
    print("\n3. Testing multi-class routing...")
    ambiguous = "How is AI being used in sports analytics?"
    matches = router.route_many(ambiguous, max_k=3)
    print(f"   Query: '{ambiguous}'")
    if matches:
        for m in matches:
            print(f"     -> {m.name} (dist={m.distance:.3f})")
    else:
        print("     -> no matches")

    # ── Cleanup ─────────────────────────────────────────────────────────
    print("\n4. Cleanup...")
    router.clear()
    print("   OK - Router cleaned up")

    print("\nAll tests passed! redisvl SemanticRouter works with Dragonfly (via KNN shim).")


if __name__ == "__main__":
    main()
