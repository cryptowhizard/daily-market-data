# daily-market-data

[![GitHub repo](https://img.shields.io/badge/GitHub-daily--market--data-blue?logo=github)](https://github.com/cryptowhizard/daily-market-data)

**`daily-market-data`** is a Node.js project that generates comprehensive daily cryptocurrency market data reports. It leverages the CoinGecko API (supporting both free and Pro modes) to collect, aggregate, and analyze a broad set of crypto market signals and statistics. This repository is particularly useful for traders, analysts, and enthusiasts who need up-to-date, locally processed, and curated datasets for market insight or downstream automation.

## Features

- **Fetches top 500 crypto assets** by market capitalization, including price, 24hr and 7d changes, and market trends.
- **Generates daily datasets**: Saves detailed market snapshots for each day, as well as a rolling `latest.json` for convenient access.
- **Narrative analysis**: Categorizes cryptocurrencies by popular investment trends (AI, DeFi, L1, L2, RWA, Gaming, Meme, Privacy, and more) and reports performance within each narrative.
- **Global metrics**: Reports on overall market capitalization, volume, BTC and ETH dominance, and more.
- **Technical signals**: Includes on-demand calculations for top gainers/losers, moving average crossovers, and coin-to-coin price correlations.
- **Built-in support for CoinGecko Free and Pro modes**: Just set the `COINGECKO_API_KEY` environment variable for Pro functionality.
- **Error handling & resilient retries:** Designed to work with both free and Pro CoinGecko API rate-limits and temporary errors.

## Getting Started

### Prerequisites

- **Node.js** (tested on v16+)
- **npm** (Node package manager)

### Installation

```bash
git clone https://github.com/cryptowhizard/daily-market-data.git
cd daily-market-data
npm install
```

### Usage

To generate the latest daily market data, run:

```bash
node scripts/generate-daily-data.js
```

**Environment variables:**

- `COINGECKO_API_KEY` (optional): Provide your CoinGecko API key for Pro API access and faster, more reliable data collection.

Example:

```bash
COINGECKO_API_KEY=your_key_here node scripts/generate-daily-data.js
```

### Output

- Data is saved in a `/data` directory at the project root:
    - `crypto-data-YYYY-MM-DD.json` (daily snapshot)
    - `latest.json` (current/latest snapshot)

Each JSON file contains structured data such as:
- Top 24h & 7d gainers/losers
- Global market metrics (market cap, volume, dominance, etc.)
- BTC/ETH summary stats and trends
- Narrative-based performance clusters
- Technical indicators and correlations

## Project Structure

- `scripts/generate-daily-data.js` - Main logic to fetch, process, and store market data.
- `data/` - Generated daily (and latest) JSON files.

## Customization

You can modify the `narratives` in `generate-daily-data.js` to update which coins belong to which investment themes.

## Language Composition

- **JavaScript:** 100%

## License

_Repository currently does not declare a license. Please contact the project owner for licensing details._

---

**Project by [cryptowhizard](https://github.com/cryptowhizard)**  
For questions, feature requests, or to report issues, open an issue on the [GitHub Issues page](https://github.com/cryptowhizard/daily-market-data/issues).
