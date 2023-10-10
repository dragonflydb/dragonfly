#!/usr/bin/env python3

"""Simulate throwing balls into bins."""

import numpy as np
import argparse
import matplotlib.pyplot as plt


def simulate_balls_into_bins(balls: int, bins: int, threshold: int, exact, trials=10000):
    """Simulate throwing M balls into N bins for a given number of trials."""
    counts = np.zeros(bins, dtype=int)
    success = 0
    exact_success = 0
    deltas = []

    for _ in range(trials):
        # Reset counts for each trial
        counts.fill(0)

        # Throw M balls into the bins
        bins_seq = np.random.randint(0, bins, balls)
        unique, counts_bins = np.unique(bins_seq, return_counts=True)
        counts[unique] += counts_bins
        deltas.append(counts.max() - counts.min())
        # Check if any bin has K or more balls
        if np.any(counts >= threshold):
            success += 1
        if exact is not None:
            if np.any(counts == exact):
                exact_success += 1

    probability = success / trials
    return deltas, probability, exact_success / trials


def main():
    parser = argparse.ArgumentParser(description="Simulate throwing balls into bins.")
    parser.add_argument("--balls", type=int, default=30, help="Number of balls to throw.")
    parser.add_argument("--bins", type=int, default=3, help="Number of bins.")
    parser.add_argument(
        "--high-threshold",
        type=int,
        default=15,
        help="Minimum number of balls for the success condition",
    )
    parser.add_argument(
        "--exact-num", type=int, help="Exact number of balls for the success condition."
    )
    parser.add_argument(
        "--trials", type=int, default=10000, help="Number of trials. Default is 10,000."
    )

    args = parser.parse_args()

    deltas, atleast_p, exact_p = simulate_balls_into_bins(
        args.balls, args.bins, args.high_threshold, args.exact_num, args.trials
    )

    print(f"Probability that at least one bin has {args.high_threshold} or more balls: {atleast_p}")
    if args.exact_num is not None:
        print(f"Probability that at least one bin has {args.exact_num} balls: {exact_p}")

    print(
        f"Histogram of the difference between the most and least populated bins for {args.trials} trials"
    )
    plt.hist(deltas, bins=30, color="steelblue", edgecolor="none")
    plt.show()


if __name__ == "__main__":
    main()
