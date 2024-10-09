import numpy as np

def test_weight_distribution(weights):
    total_weights = len(weights)
    sorted_weights = sorted(weights, reverse=True)

    # Calculate the number of weights in each tier
    top_10_percent = int(0.1 * total_weights)
    next_40_percent = int(0.4 * total_weights)
    bottom_50_percent = total_weights - top_10_percent - next_40_percent

    # Split the weights into tiers
    top_tier = sorted_weights[:top_10_percent]
    middle_tier = sorted_weights[top_10_percent:top_10_percent + next_40_percent]
    bottom_tier = sorted_weights[top_10_percent + next_40_percent:]

    # Calculate the sum of weights in each tier
    top_tier_sum = sum(top_tier)
    middle_tier_sum = sum(middle_tier)
    bottom_tier_sum = sum(bottom_tier)
    total_sum = sum(weights)

    # Calculate the percentage of total weight for each tier
    top_tier_percentage = (top_tier_sum / total_sum) * 100
    middle_tier_percentage = (middle_tier_sum / total_sum) * 100
    bottom_tier_percentage = (bottom_tier_sum / total_sum) * 100

    # Print results
    print(f"Top 10% ({top_10_percent} weights): {top_tier_percentage:.2f}% of total weight (Expected: 70%)")
    print(f"Next 40% ({next_40_percent} weights): {middle_tier_percentage:.2f}% of total weight (Expected: 20%)")
    print(f"Bottom 50% ({bottom_50_percent} weights): {bottom_tier_percentage:.2f}% of total weight (Expected: 10%)")

    print("\nTop 10% of weights:")
    for i, weight in enumerate(top_tier, 1):
        print(f"{i}. {weight:.6f}")

# Example usage
weights = [
    0.0099, 0.0099, 0.0099, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0099,
    0.0101, 0.0243, 0.0097, 0.0097, 0.0285, 0.0099, 0.0285, 0.0285, 0.0099,
    0.0284, 0.0284, 0.0030, 0.0284, 0.0284, 0.0284, 0.0280, 0.0008, 0.0008,
    0.0285, 0.0285, 0.0248, 0.0285, 0.0285, 0.0102, 0.0102, 0.0102, 0.0102,
    0.0208, 0.0208, 0.0077, 0.0209, 0.0259, 0.0008, 0.0070, 0.0070, 0.0070,
    0.0066, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0064, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0341, 0.0008, 0.0008, 0.0008, 0.0008, 0.0066, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0349, 0.0008, 0.0080, 0.0008, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0316, 0.0000, 0.0000, 0.0000, 0.0299, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0000, 0.0000, 0.0319, 0.0000, 0.0303, 0.0000, 0.0000, 0.0046,
    0.0064, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0008, 0.0000,
    0.0000, 0.0000, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0000, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008,
    0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0008, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 0.0000,
    0.0000, 0.0000, 0.0000, 0.0000
]
test_weight_distribution(weights)
