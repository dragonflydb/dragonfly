"""
Dragonfly-compatible SemanticRouter shim.

Subclasses redisvl's SemanticRouter and replaces the FT.AGGREGATE + VECTOR_RANGE
query path with FT.SEARCH + KNN plus client-side aggregation.

This works around two missing Dragonfly features:
  1. VECTOR_RANGE query syntax (only KNN is supported)
  2. Several FT.AGGREGATE clauses (SCORER, DIALECT, FILTER)

Usage:
    from dragonfly_router import DragonflySemanticRouter

    router = DragonflySemanticRouter(
        name="my-router",
        routes=[...],
        redis_url="redis://localhost:6379",
    )
    match = router("some query")
"""

import os
import struct
import sys
from collections import defaultdict
from typing import Dict, List, Optional

DEBUG = os.environ.get("DRAGONFLY_ROUTER_DEBUG", "0") == "1"

from redisvl.extensions.router import SemanticRouter
from redisvl.extensions.router.schema import DistanceAggregationMethod, RouteMatch


class DragonflySemanticRouter(SemanticRouter):
    """SemanticRouter that uses KNN search instead of VECTOR_RANGE + FT.AGGREGATE."""

    def _get_route_matches(
        self,
        vector: List[float],
        aggregation_method: DistanceAggregationMethod,
        max_k: int = 1,
    ) -> List[RouteMatch]:
        """Override: use FT.SEARCH with KNN + client-side aggregation."""

        # Total number of reference vectors across all routes
        total_refs = sum(len(route.references) for route in self.routes)
        if total_refs == 0:
            return []

        # Build per-route distance threshold lookup
        thresholds: Dict[str, float] = {
            route.name: route.distance_threshold for route in self.routes
        }

        # Encode query vector as FLOAT32 bytes
        vector_bytes = struct.pack(f"<{len(vector)}f", *vector)

        # Use FT.SEARCH with KNN to get all references ranked by distance
        client = self._index.client
        index_name = self._index.schema.index.name

        results = client.execute_command(
            "FT.SEARCH",
            index_name,
            f"*=>[KNN {total_refs} @vector $query_vec AS vector_distance]",
            "PARAMS",
            "2",
            "query_vec",
            vector_bytes,
            "SORTBY",
            "vector_distance",
            "ASC",
            "RETURN",
            "2",
            "route_name",
            "vector_distance",
            "LIMIT",
            "0",
            str(total_refs),
        )

        # Parse FT.SEARCH results: [total, key1, [f, v, ...], key2, [f, v, ...], ...]
        if DEBUG:
            print(f"[DEBUG] FT.SEARCH returned {len(results)} elements", file=sys.stderr)
            print(f"[DEBUG] results[0] (count) = {results[0]}", file=sys.stderr)
            for i in range(1, min(len(results), 7), 2):
                print(f"[DEBUG]   key={results[i]}, fields={results[i+1]}", file=sys.stderr)

        route_distances: Dict[str, List[float]] = defaultdict(list)

        for i in range(1, len(results), 2):
            fields = results[i + 1]
            field_map = {}
            for j in range(0, len(fields), 2):
                k = fields[j].decode() if isinstance(fields[j], bytes) else fields[j]
                v = fields[j + 1].decode() if isinstance(fields[j + 1], bytes) else fields[j + 1]
                field_map[k] = v

            rname = field_map.get("route_name", "")
            dist = float(field_map.get("vector_distance", "inf"))

            # Apply per-route distance threshold (mirrors the FILTER clause)
            if rname in thresholds and dist < thresholds[rname]:
                route_distances[rname].append(dist)

        # Aggregate distances per route
        route_matches: List[RouteMatch] = []
        for rname, dists in route_distances.items():
            if aggregation_method == DistanceAggregationMethod.min:
                agg_dist = min(dists)
            elif aggregation_method == DistanceAggregationMethod.sum:
                agg_dist = sum(dists)
            else:  # avg (default)
                agg_dist = sum(dists) / len(dists)

            route_matches.append(RouteMatch(name=rname, distance=agg_dist))

        # Sort by distance ascending, limit to max_k
        route_matches.sort(key=lambda m: m.distance)
        return route_matches[:max_k]
