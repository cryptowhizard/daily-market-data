const https = require('https');
const fs = require('fs');
const path = require('path');

// Get date key for New York timezone
function getDateKeyNY() {
  const now = new Date();
  const fmt = new Intl.DateTimeFormat('en-CA', { 
    timeZone: 'America/New_York', 
    year: 'numeric', 
    month: '2-digit', 
    day: '2-digit' 
  });
  const parts = fmt.formatToParts(now).reduce((acc, p) => { 
    acc[p.type] = p.value; 
    return acc; 
  }, {});
  return `${parts.year}-${parts.month}-${parts.day}`;
}

class CryptoDataGenerator {
  constructor() {
    this.API_KEY = process.env.COINGECKO_API_KEY;
    this.BASE_URL = this.API_KEY ? 'https://pro-api.coingecko.com/api/v3' : 'https://api.coingecko.com/api/v3';
    this.cache = new Map();
    this.CACHE_DURATION = 5 * 60 * 1000; // 5 minutes
    
    console.log('🔑 GitHub Actions - API Configuration:');
    console.log(`   - Using ${this.API_KEY ? 'PRO' : 'FREE'} CoinGecko API`);
    console.log(`   - Base URL: ${this.BASE_URL}`);
  }

  // HTTP request function
  makeRequest(url, headers = {}) {
    return new Promise((resolve, reject) => {
      const requestHeaders = {
        'User-Agent': 'Mozilla/5.0 (compatible; CryptoTracker/1.0; +https://github.com/crypto-market-tracker)',
        ...headers
      };

      if (this.API_KEY) {
        requestHeaders['x-cg-pro-api-key'] = this.API_KEY;
      }

      const options = { headers: requestHeaders };

      https.get(url, options, (res) => {
        let data = '';
        
        res.on('data', (chunk) => {
          data += chunk;
        });
        
        res.on('end', () => {
          if (res.statusCode === 401) {
            reject(new Error('Unauthorized - Check your API key'));
            return;
          }
          
          if (res.statusCode === 429) {
            reject(new Error('Rate limited - Too many requests'));
            return;
          }
          
          if (res.statusCode !== 200) {
            reject(new Error(`HTTP ${res.statusCode}: ${data}`));
            return;
          }

          try {
            const parsed = JSON.parse(data);
            resolve(parsed);
          } catch (error) {
            reject(new Error(`JSON parse error: ${error.message}`));
          }
        });
      }).on('error', (error) => {
        reject(error);
      });
    });
  }

  async makeAPICall(endpoint, params = {}) {
    const queryString = new URLSearchParams(params).toString();
    const url = `${this.BASE_URL}${endpoint}${queryString ? '?' + queryString : ''}`;
    const cacheKey = url;
    
    const cached = this.cache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      console.log(`📦 Cache hit for: ${endpoint}`);
      return cached.data;
    }

    try {
      console.log(`🌐 API call: ${endpoint}`);
      const data = await this.makeRequest(url);
      
      this.cache.set(cacheKey, {
        data: data,
        timestamp: Date.now()
      });
      
      // Add delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      return data;
    } catch (error) {
      console.error(`❌ API call failed: ${endpoint}`, error.message);
      
      if (cached) {
        console.log('📦 Using stale cache data');
        return cached.data;
      }
      
      throw error;
    }
  }

  async getTopPerformers() {
    console.log('📈 Fetching top 500 coins...');
    const allCoins = [];
    const perPage = 250; // Maximum allowed by CoinGecko API
    const totalCoins = 500;
    const pages = Math.ceil(totalCoins / perPage);

    // Fetch all pages of data
    for (let page = 1; page <= pages; page++) {
      const coinsToFetch = page === pages ? (totalCoins % perPage || perPage) : perPage;
      console.log(`   - Fetching page ${page} (${coinsToFetch} coins)...`);
      
      const data = await this.makeAPICall('/coins/markets', {
        vs_currency: 'usd',
        order: 'market_cap_desc',
        per_page: coinsToFetch,
        page: page,
        sparkline: false,
        price_change_percentage: '24h,7d'
      });

      if (!Array.isArray(data)) {
        console.error('❌ Invalid data format from API:', data);
        continue;
      }

      allCoins.push(...data);
      
      // Add delay between requests to avoid rate limiting
      if (page < pages) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    if (allCoins.length === 0) {
      throw new Error('No coin data available');
    }

    console.log(`✅ Fetched ${allCoins.length} coins in total`);

    // Process the data
    const topGainers24h = allCoins
      .filter(coin => coin.price_change_percentage_24h > 0)
      .sort((a, b) => b.price_change_percentage_24h - a.price_change_percentage_24h)
      .slice(0, 15); // Get more for better selection

    const topLosers24h = allCoins
      .filter(coin => coin.price_change_percentage_24h < 0)
      .sort((a, b) => a.price_change_percentage_24h - b.price_change_percentage_24h)
      .slice(0, 15);

    const topGainers7d = allCoins
      .filter(coin => coin.price_change_percentage_7d_in_currency != null && coin.price_change_percentage_7d_in_currency > 0)
      .sort((a, b) => b.price_change_percentage_7d_in_currency - a.price_change_percentage_7d_in_currency)
      .slice(0, 15);

    console.log(`✅ Found ${topGainers24h.length} gainers, ${topLosers24h.length} losers, ${topGainers7d.length} weekly winners`);
    
    return { 
      topGainers24h, 
      topLosers24h, 
      topGainers7d,
      allCoins // Include all coins for additional processing
    };
  }

  async getGlobalMetrics() {
    console.log('🌍 Fetching global metrics...');
    try {
      const data = await this.makeAPICall('/global');
      
      if (!data.data) {
        throw new Error('Invalid global data format');
      }
      
      const btcDominance = data.data.market_cap_percentage?.btc || 0;
      const ethDominance = data.data.market_cap_percentage?.eth || 0;
      const totalMarketCap = data.data.total_market_cap?.usd || 0;
      const total3MarketCap = totalMarketCap * (1 - ((btcDominance + ethDominance) / 100));
      
      console.log(`✅ BTC dominance: ${btcDominance.toFixed(1)}%, Total cap: ${this.formatNumber(totalMarketCap)}`);
      
      return {
        btcDominance,
        ethDominance,
        totalMarketCap,
        total3MarketCap,
        volume24h: data.data.total_volume?.usd || 0,
        activeCryptocurrencies: data.data.active_cryptocurrencies || 0,
        totalVolume: data.data.total_volume?.usd || 0,
        marketCapChange24h: data.data.market_cap_change_percentage_24h_usd || 0
      };
    } catch (error) {
      console.error('❌ Error fetching global metrics:', error.message);
      throw error;
    }
  }

  async getBTCData() {
    console.log('₿ Fetching BTC data...');
    try {
      const data = await this.makeAPICall('/coins/bitcoin', {
        localization: false,
        tickers: false,
        market_data: true,
        community_data: false,
        developer_data: false,
        sparkline: false
      });
      
      const currentPrice = data.market_data?.current_price?.usd || 0;
      const priceChange24h = data.market_data?.price_change_percentage_24h || 0;
      const marketCap = data.market_data?.market_cap?.usd || 0;
      
      // Determine trend based on price change
      let trend = 'neutral';
      if (priceChange24h > 2) trend = 'strong_bull';
      else if (priceChange24h > 0.5) trend = 'bullish';
      else if (priceChange24h < -2) trend = 'strong_bear';
      else if (priceChange24h < -0.5) trend = 'bearish';
      
      console.log(`✅ BTC: $${currentPrice.toLocaleString()} (${priceChange24h > 0 ? '+' : ''}${priceChange24h.toFixed(2)}%), Trend: ${trend}`);
      
      return {
        currentPrice,
        priceChange24h,
        marketCap,
        trend
      };
    } catch (error) {
      console.error('⚠️ BTC data error:', error.message);
      throw error;
    }
  }

  async getNarrativeData() {
    console.log('📊 Generating narrative data...');
    
    // Define narratives with their associated coins
    const narratives = {
      'AI': [
        'fet-ai', 'the-graph', 'ocean-protocol', 'singularitynet', 
        'fetch-ai', 'numerai', 'render-token', 'akash-network',
        'helium', 'theta-token', 'filecoin'
      ],
      'DeFi': [
        'uniswap', 'aave', 'curve-dao-token', 'compound-governance-token', 
        'synthetix-network-token', 'balancer', 'yearn-finance', 'maker',
        'lido-dao', 'rocket-pool', 'frax-ether'
      ],
      'L1': [
        'solana', 'avalanche-2', 'polkadot', 'cosmos', 'algorand', 
        'near', 'aptos', 'sui', 'hedera-hashgraph'
      ],
      'L2': [
        'arbitrum', 'optimism', 'matic-network', 'loopring', 'immutable-x',
        'starknet', 'metis-token'
      ],
      'RWA': [
        'chainlink', 'the-graph', 'ocean-protocol', 'injective-protocol', 
        'band-protocol', 'centrifuge', 'goldfinch', 'maple'
      ],
      'Gaming': [
        'axie-infinity', 'the-sandbox', 'decentraland', 'gala', 'illuvium',
        'wax', 'ultra'
      ],
      'Meme': [
        'dogecoin', 'shiba-inu', 'pepe', 'floki', 'bonk',
        'dogwifhat', 'babydoge'
      ],
      'Privacy': [
        'monero', 'zcash', 'horizen', 'secret', 'beam',
        'dusk-network', 'railgun'
      ]
    };

    const narrativeData = {};
    
    try {
      const coinIds = Object.values(narratives).flat();
      const uniqueCoinIds = [...new Set(coinIds)]; // Remove duplicates
      
      // Fetch data in batches to avoid URL length limits
      const batchSize = 100;
      const batches = [];
      for (let i = 0; i < uniqueCoinIds.length; i += batchSize) {
        batches.push(uniqueCoinIds.slice(i, i + batchSize));
      }
      
      let allCoinData = [];
      for (const batch of batches) {
        console.log(`   - Fetching batch of ${batch.length} narrative coins...`);
        const response = await this.makeAPICall('/coins/markets', {
          vs_currency: 'usd',
          ids: batch.join(','),
          price_change_percentage: '24h,7d',
          per_page: 250
        });
        
        if (Array.isArray(response)) {
          allCoinData.push(...response);
        }
        
        // Add delay between batches
        await new Promise(resolve => setTimeout(resolve, 1500));
      }
      
      // Create a map of coin data for easy access
      const coinDataMap = {};
      allCoinData.forEach(coin => {
        coinDataMap[coin.id] = {
          id: coin.id,
          name: coin.name,
          symbol: coin.symbol.toUpperCase(),
          current_price: coin.current_price,
          price_change_percentage_24h: (coin.price_change_percentage_24h_in_currency ?? coin.price_change_percentage_24h ?? 0),
          price_change_percentage_7d: (coin.price_change_percentage_7d_in_currency ?? 0),
          market_cap: coin.market_cap || 0,
          image: coin.image
        };
      });
      
      // Process each narrative
      for (const [narrative, coinIds] of Object.entries(narratives)) {
        const coins = [];
        let totalMarketCap = 0;
        let total24hChange = 0;
        let total7dChange = 0;
        let coinCount = 0;
        
        // Process each coin in the narrative
        for (const coinId of coinIds) {
          if (coinDataMap[coinId]) {
            const coin = coinDataMap[coinId];
            coins.push(coin);
            totalMarketCap += coin.market_cap || 0;
            total24hChange += coin.price_change_percentage_24h || 0;
            total7dChange += coin.price_change_percentage_7d || 0;
            coinCount++;
          }
        }
        
        // Calculate narrative metrics
        const avg24hChange = coinCount > 0 ? total24hChange / coinCount : 0;
        const avg7dChange = coinCount > 0 ? total7dChange / coinCount : 0;
        
        // Sort coins by 24h performance
        const sortedCoins = [...coins].sort((a, b) => 
          (b.price_change_percentage_24h || 0) - (a.price_change_percentage_24h || 0)
        );
        
        // Only include narratives with at least 3 coins
        if (coinCount >= 3) {
          narrativeData[narrative] = {
            change24h: parseFloat(avg24hChange.toFixed(2)),
            change7d: parseFloat(avg7dChange.toFixed(2)),
            marketCap: totalMarketCap,
            coinCount,
            topPerformers: sortedCoins.slice(0, 3).map(coin => ({
              id: coin.id,
              symbol: coin.symbol,
              name: coin.name,
              change24h: coin.price_change_percentage_24h,
              price: coin.current_price
            }))
          };
        }
      }
      
      console.log(`✅ Generated data for ${Object.keys(narrativeData).length} narratives`);
      return narrativeData;
      
    } catch (error) {
      console.error('Error fetching narrative data:', error);
      throw error;
    }
  }

  formatNumber(num, decimals = 2) {
    if (num >= 1e12) return (num / 1e12).toFixed(decimals) + 'T';
    if (num >= 1e9) return (num / 1e9).toFixed(decimals) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(decimals) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(decimals) + 'K';
    return num.toFixed(decimals);
  }

  ensureDirectoryExists(dirPath) {
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
    }
  }

  async generateDailyData() {
    try {
      console.log('🚀 Starting daily crypto data generation...');
      console.log('🔑 API Key configured:', this.API_KEY ? '✅ Yes (Pro Mode)' : '❌ No (Free Mode)');
      
      const startTime = Date.now();
      
      // Fetch all required data
      const topPerformers = await this.getTopPerformers();
      const globalMetrics = await this.getGlobalMetrics();
      const btcData = await this.getBTCData();
      const narrativeData = await this.getNarrativeData();
      
      // Prepare complete data structure
      const dailyData = {
        date: getDateKeyNY(),
        timestamp: new Date().toISOString(),
        topGainers24h: topPerformers.topGainers24h,
        topLosers24h: topPerformers.topLosers24h,
        topGainers7d: topPerformers.topGainers7d,
        globalMetrics,
        btcData,
        narrativeData,
        metadata: {
          totalCoinsAnalyzed: topPerformers.allCoins.length,
          apiMode: this.API_KEY ? 'pro' : 'free',
          generatedBy: 'github-actions',
          version: '2.0'
        }
      };

      console.log('📊 Data collection completed:');
      console.log(`   - Top gainers: ${dailyData.topGainers24h.length}`);
      console.log(`   - Top losers: ${dailyData.topLosers24h.length}`);
      console.log(`   - Weekly winners: ${dailyData.topGainers7d.length}`);
      console.log(`   - BTC price: $${dailyData.btcData.currentPrice?.toLocaleString()}`);
      console.log(`   - Market cap: ${this.formatNumber(dailyData.globalMetrics.totalMarketCap)}`);
      console.log(`   - Narratives: ${Object.keys(dailyData.narrativeData).length}`);

      // Ensure data directory exists
      const dataDir = path.join(process.cwd(), 'data');
      this.ensureDirectoryExists(dataDir);
      
      // Save to dated file
      const fileName = `crypto-data-${getDateKeyNY()}.json`;
      const filePath = path.join(dataDir, fileName);
      
      fs.writeFileSync(filePath, JSON.stringify(dailyData, null, 2));
      
      // Also save as latest.json for easy access
      const latestPath = path.join(dataDir, 'latest.json');
      fs.writeFileSync(latestPath, JSON.stringify(dailyData, null, 2));
      
      const endTime = Date.now();
      const duration = ((endTime - startTime) / 1000).toFixed(2);
      
      console.log('');
      console.log('✅ Daily data generated successfully!');
      console.log(`⏱️  Generation time: ${duration} seconds`);
      console.log(`📁 Saved to: ${filePath}`);
      console.log(`📁 Latest: ${latestPath}`);
      console.log('');
      
      return {
        success: true,
        message: 'Daily data generated successfully',
        duration: duration + 's',
        timestamp: dailyData.timestamp,
        filePath,
        dataPoints: {
          gainers: dailyData.topGainers24h.length,
          losers: dailyData.topLosers24h.length,
          weeklyWinners: dailyData.topGainers7d.length,
          narratives: Object.keys(dailyData.narrativeData).length
        }
      };
      
    } catch (error) {
      console.error('❌ Error generating daily data:', error.message);
      console.error('Stack trace:', error.stack);
      return { success: false, error: error.message };
    }
  }
}

// Main execution
async function main() {
  const generator = new CryptoDataGenerator();
  const result = await generator.generateDailyData();
  
  if (!result.success) {
    console.error('💥 Data generation failed:', result.error);
    process.exit(1);
  }
  
  console.log('🎉 Data generation completed successfully!');
  process.exit(0);
}

// Run if this file is executed directly
if (require.main === module) {
  main().catch(error => {
    console.error('💥 Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { CryptoDataGenerator, getDateKeyNY };
