<div align="center">

# **Nextplace AI** <!-- omit in toc -->

## Real Estate Market Research <!-- omit in toc -->

[Discord](https://discord.gg/xbRhw9jX) • [Taostats](https://taostats.io/subnets/48/metagraph) • [Website](https://nextplace.ai/)
</div>

# Nextplace AI 🏡

Nextplace AI is decentralizing intelligence around housing markets. In a space controlled by monopolies and gatekeepers, Nexplace seeks to provide a democratized network to evaluate home prices for everybody.

## Miners

Miners will develop their own models to predict home prices and sales dates. They can use data provided by the validators or call out to API's to gather more data for their models inference. Miners will provide the expected sales date and the predicted home price.

## Validators

Validators provide data to miners from 50 markets. This number will expand over time. Validators evaluate miners based on their accuracy in prediction of home price and sales date.

### Scoring Method for Home Price Prediction

The scoring system calculates a miner's prediction score based on two key factors: the accuracy of the predicted home price and the accuracy of the predicted sale date. The final score combines these components with weighted contributions (86% for price accuracy, 14% for date accuracy). A perfect prediction would result in a score of 100.

### Score Calculation:

1. **Price Accuracy (86% weight):**
   - The difference between the actual and predicted prices is calculated as a percentage of the actual price.
   - The price score is then calculated as:  
     `Price Score = max(0, 100 - (price difference percentage * 100))`

2. **Date Accuracy (14% weight):**
   - The difference between the actual and predicted sale dates is measured in days.
   - Each day of difference reduces the score by a set amount, with the maximum score being 100 and decreasing linearly up to 14 days.
   - The date score is calculated as:  
     `Date Score = (max(0, 14 - date difference) / 14) * 100`

3. **Final Score:**
   - The final score is a weighted combination of the price and date scores:  
     `Final Score = (Price Score * 0.86) + (Date Score * 0.14)`

### Example:
- **Predicted Price**: \$400,000
- **Actual Price**: \$420,000
- **Predicted Sale Date**: 2023-09-01
- **Actual Sale Date**: 2023-09-05

**Steps:**

1. **Price Score Calculation**:  
   Price difference = \$20,000  
   Price difference percentage = 20,000 / 420,000 = 0.0476  
   Price Score = 100 - (0.0476 * 100) = 95.24

2. **Date Score Calculation**:  
   Date difference = 4 days  
   Date Score = (14 - 4) / 14 * 100 = 71.43

3. **Final Score Calculation**:  
   Final Score = (95.24 * 0.86) + (71.43 * 0.14) = 92.16

The average performance on all sold homes in the last 30 days will be used to calculate incentive.

#### Weight Calculation using Exponential Decay:

Validators reward the best miners disproportionally. The weights decay drastically as miners become less accurate.

The top 10% of miners receive 40% of total rewards, and the next 40% of miners get 40% of rewards. The bottom 50% of miners receive just 20% of rewards since they are not providing differentiated value.

## Installation 

[Miners](nextplace/miner/README.md)

[Validators](nextplace/validator/README.md)

# Nexplace Roadmap

- Expand Markets
  - Nexplace will add markets over time, requiring miners to develop better and more generalizable models. This will also provide more value to Nextplace consumers.

- Visualize Sales-to-Listing data
  - The Nexplace website will be a hub for visualizing different markets and providing alpha to home buyers and sellers. The first step here will be displaying the difference in home prices and home sales, as well as the top miners' predictions for current listings.

- Price My Home feature
  - Nextplace will allow a user to get an estimate of their home through the website. The request will be forwarded to the top miners to get an estimate of their home, or a home they wish to purchase. This data will give a buyer or seller a better understanding of their transaction and the real value of a home.