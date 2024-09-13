<div align="center">

# **Nextplace AI** <!-- omit in toc -->

## Real Estate Market Research <!-- omit in toc -->

[Discord](https://discord.gg/bittensor) • [Network](https://taostats.io/) • [Website](https://nextplace.ai/)
</div>

---
- [Quickstarter template](#quickstarter-template)
- [Introduction](#introduction)
  - [Example](#example)
- [Installation](#installation)
  - [Before you proceed](#before-you-proceed)
  - [Install](#install)
- [Writing your own incentive mechanism](#writing-your-own-incentive-mechanism)
- [Writing your own subnet API](#writing-your-own-subnet-api)
- [Subnet Links](#subnet-links)
- [License](#license)

---
# Nextplace AI 🏡

Nextplace AI is decentralizing intelligence around housing markets. In a space controlled by monopolies and gatekeepers, Nexplace seeks to provide a democratized network to evaluate home prices for the general market and individuals.

## Miners

Miners will develop their own models to predict home prices and sales dates. They can use data provided by the validators or call out to API's to gather more data for their models inference. Mienrs will provide the expected sales date and the predicted home price.

## Validators

Validators provide data to miners from <INSERT NUMBER OF MARKETS> markets. This number will expand over time. Validators evaluate miners based on their accuracy in prediction of home price and sales date.

## Scoring *THIS NEEDS REVISION

### Scoring Method for Home Price Prediction (Normalized)

This function calculates a normalized score based on how close a predicted home price is to the actual home price, with the score ranging from 0 (worst) to 1 (perfect prediction). The score is divided into two components: the price difference (80%) and the date difference (20%).

### Formula:
1. **Price difference** (80% of the score): 
   - Calculate the absolute price difference between the predicted price and the actual home price.
   - Divide the price difference by the maximum possible price difference (or a set threshold), and then multiply by 0.8.

2. **Time difference** (20% of the score):
   - Calculate the absolute difference in days between the prediction and the actual home price date, capped at a maximum of 20 days.
   - Divide the day difference by 20 (since that's the max), and multiply by 0.2.

3. **Final Score**: Subtract the sum of the normalized price and time differences from 1.

#### Example:
- **Predicted Price**: \$400,000
- **Actual Price**: \$420,000
- **Max Price Difference**: \$100,000 (as an example threshold)
- **Price Difference**: \$20,000
- **Days Difference**: 10 days

##### Score Calculation:
- Normalized Price Difference: `20,000 / 100,000 = 0.2`
- Price Difference Score: `0.2 * 0.8 = 0.16`
  
- Normalized Time Difference: `10 / 20 = 0.5`
- Time Difference Score: `0.5 * 0.2 = 0.1`

**Total Normalized Score** = `1 - (0.16 + 0.1) = 0.74`

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