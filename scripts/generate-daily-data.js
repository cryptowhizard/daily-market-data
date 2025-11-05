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
    this.isPro = !!this.API_KEY;
    this.BASE_URL = this.isPro ? 'https://pro-api.coingecko.com/api/v3' : 'https://api.coingecko.com/api/v3';
    this.cache = new Map();
    this.CACHE_DURATION = 5 * 60 * 1000; // 5 minutes
    this.minDelayMs = this.isPro ? 300 : 1600; // minimal spacing between calls
    this.maxRetries = this.isPro ? 3 : 6; // retry more on free
    this.backoffBaseMs = this.isPro ? 500 : 2000;
    this.lastRequestTime = 0;
    this.freeModeForced = false; // becomes true if we auto-downgrade from PRO

    console.log('🔑 GitHub Actions - API Configuration:');
    console.log(`   - Using ${this.isPro ? 'PRO' : 'FREE'} CoinGecko API`);
    console.log(`   - Base URL: ${this.BASE_URL}`);
    console.log(`   - Min delay: ${this.minDelayMs}ms, Max retries: ${this.maxRetries}`);
  }

  // === Technical helpers reused from API ===
  calculateEMA(prices, period) {
    const k = 2 / (period + 1);
    const sma = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
    const ema = Array(period - 1).fill(null);
    ema.push(sma);
    for (let i = period; i < prices.length; i++) {
      ema.push((prices[i] * k) + (ema[ema.length - 1] * (1 - k)));
    }
    return ema;
  }

  calculateCorrelationAndBeta(prices1, prices2) {
    if (prices1.length !== prices2.length || prices1.length < 2) {
      return { correlation: 0, beta: 0 };
    }
    const n = prices1.length;
    let sum1 = 0, sum2 = 0;
    for (let i = 0; i < n; i++) { sum1 += prices1[i]; sum2 += prices2[i]; }
    const mean1 = sum1 / n;
    const mean2 = sum2 / n;
    let cov = 0, var1 = 0, var2 = 0;
    for (let i = 0; i < n; i++) {
      const d1 = prices1[i] - mean1;
      const d2 = prices2[i] - mean2;
      cov += d1 * d2;
      var1 += d1 * d1;
      var2 += d2 * d2;
    }
    if (var1 === 0 || var2 === 0) {
      return { correlation: 0, beta: 0 };
    }
    const correlation = cov / Math.sqrt(var1 * var2);
    const beta = cov / var2;
    return { correlation, beta };
  }

  calculateDownsideBeta(returns, marketReturns) {
    let sumProduct = 0;
    let sumSquaredMarket = 0;
    for (let i = 0; i < marketReturns.length; i++) {
      if (marketReturns[i] < 0) {
        sumProduct += returns[i] * marketReturns[i];
        sumSquaredMarket += marketReturns[i] * marketReturns[i];
      }
    }
    return sumSquaredMarket === 0 ? 0 : sumProduct / sumSquaredMarket;
  }

  // === Rate limit & retry helpers ===
  sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  async enforceRateLimit() {
    const now = Date.now();
    const elapsed = now - this.lastRequestTime;
    if (elapsed < this.minDelayMs) {
      await this.sleep(this.minDelayMs - elapsed);
    }
    this.lastRequestTime = Date.now();
  }

  randomJitter(ms) {
    const jitter = Math.floor(Math.random() * Math.min(500, Math.max(100, ms * 0.1)));
    return ms + jitter;
  }

  computeBackoffDelay(attempt, retryAfterHeader) {
    if (retryAfterHeader) {
      const ra = Number(retryAfterHeader);
      if (!isNaN(ra) && ra > 0) return (ra * 1000);
    }
    // exponential backoff with cap
    const base = this.backoffBaseMs * Math.pow(2, attempt);
    return Math.min(30000, this.randomJitter(base));
  }

  downgradeToFree() {
    if (!this.isPro) return;
    console.warn('🔁 Downgrading to FREE API due to auth failure on PRO');
    this.isPro = false;
    this.API_KEY = null;
    this.BASE_URL = 'https://api.coingecko.com/api/v3';
    this.minDelayMs = 1600;
    this.maxRetries = 6;
    this.backoffBaseMs = 2000;
    this.freeModeForced = true;
  }

  async fetchHistoricalTotal3(days = 90) {
    try {
      let btcChart, ethChart, globalNow;
      if (this.isPro) {
        [btcChart, ethChart, globalNow] = await Promise.all([
          this.makeAPICall('/coins/bitcoin/market_chart', { vs_currency: 'usd', days, interval: 'daily' }),
          this.makeAPICall('/coins/ethereum/market_chart', { vs_currency: 'usd', days, interval: 'daily' }),
          this.makeAPICall('/global')
        ]);
      } else {
        // On FREE, avoid parallel bursts
        btcChart = await this.makeAPICall('/coins/bitcoin/market_chart', { vs_currency: 'usd', days, interval: 'daily' });
        ethChart = await this.makeAPICall('/coins/ethereum/market_chart', { vs_currency: 'usd', days, interval: 'daily' });
        globalNow = await this.makeAPICall('/global');
      }
      const btcCaps = btcChart?.market_caps || [];
      const ethCaps = ethChart?.market_caps || [];
      const len = Math.min(btcCaps.length, ethCaps.length);
      if (len < 2) throw new Error('Insufficient BTC/ETH history');
      const btcD = (globalNow?.data?.market_cap_percentage?.btc || 0) / 100;
      const ethD = (globalNow?.data?.market_cap_percentage?.eth || 0) / 100;
      const denom = Math.max(btcD + ethD, 1e-6);
      const total3 = [];
      for (let i = 0; i < len; i++) {
        const totalMcap = (btcCaps[i][1] + ethCaps[i][1]) / denom;
        total3.push(totalMcap * (1 - btcD - ethD));
      }
      return total3;
    } catch (e) {
      // Fallback to flat series using snapshot
      try {
        const g = await this.makeAPICall('/global');
        const total = Number(g?.data?.total_market_cap?.usd || 0);
        const btcD = Number(g?.data?.market_cap_percentage?.btc || 0) / 100;
        const ethD = Number(g?.data?.market_cap_percentage?.eth || 0) / 100;
        const t3 = total * (1 - btcD - ethD);
        return Array.from({ length: days }, () => t3);
      } catch {
        return Array.from({ length: days }, () => 0);
      }
    }
  }

  async getCorrelationAnalysis(topCoins) {
    const results = [];
    // Limit to reduce rate limits during GitHub Actions run
    const coinsToAnalyze = (topCoins || []).slice(0, 60);
    const total3Prices = await this.fetchHistoricalTotal3(90);
    const total3Returns = [];
    for (let i = 1; i < total3Prices.length; i++) {
      const prev = total3Prices[i - 1];
      const curr = total3Prices[i];
      if (prev === 0) { total3Returns.push(0); } else { total3Returns.push((curr - prev) / prev); }
    }
    for (const coin of coinsToAnalyze) {
      try {
        if (!coin?.id || coin.id === 'bitcoin' || coin.id === 'ethereum') continue;
        const history = await this.makeAPICall(`/coins/${coin.id}/market_chart`, {
          vs_currency: 'usd', days: 90, interval: 'daily'
        });
        if (!history.prices || history.prices.length < 2) continue;
        const prices = history.prices.map(p => p[1]);
        const returns = [];
        for (let i = 1; i < prices.length; i++) {
          const prev = prices[i - 1];
          const curr = prices[i];
          returns.push(prev === 0 ? 0 : (curr - prev) / prev);
        }
        const minLen = Math.min(returns.length, total3Returns.length);
        if (minLen < 2) continue;
        const { correlation, beta } = this.calculateCorrelationAndBeta(
          returns.slice(-minLen), total3Returns.slice(-minLen)
        );
        const downsideBeta = this.calculateDownsideBeta(
          returns.slice(-minLen), total3Returns.slice(-minLen)
        );
        results.push({
          id: coin.id,
          name: coin.name,
          symbol: (coin.symbol || '').toUpperCase(),
          currentPrice: coin.current_price,
          marketCap: coin.market_cap,
          priceChange24h: coin.price_change_percentage_24h,
          correlation,
          beta,
          downsideBeta,
          timestamp: new Date().toISOString()
        });
        await this.sleep(this.isPro ? 500 : 2200);
      } catch (e) {
        console.warn(`Correlation calc failed for ${coin?.id}:`, e.message);
      }
    }
    return results.sort((a, b) => b.marketCap - a.marketCap);
  }

  async getEMACrossovers(topCoins) {
    const results = [];
    const coinsToAnalyze = (topCoins || []).slice(0, 60);
    for (const coin of coinsToAnalyze) {
      try {
        if (!coin?.id) continue;
        const history = await this.makeAPICall(`/coins/${coin.id}/market_chart`, {
          vs_currency: 'usd', days: 90, interval: 'daily'
        });
        if (!history.prices || history.prices.length < 55) continue;
        const prices = history.prices.map(p => p[1]);
        const ema21 = this.calculateEMA(prices, 21);
        const ema55 = this.calculateEMA(prices, 55);
        const lastIdx = prices.length - 1;
        const firstValidIdx = 54; // max(21,55)-1
        if (lastIdx <= firstValidIdx) continue;
        let lastCrossoverIdx = -1;
        let lastCrossoverType = 'none';
        for (let j = firstValidIdx + 1; j <= lastIdx; j++) {
          if (ema21[j] == null || ema55[j] == null || ema21[j-1] == null || ema55[j-1] == null) continue;
          const prevDiff = ema21[j-1] - ema55[j-1];
          const currDiff = ema21[j] - ema55[j];
          if (prevDiff < 0 && currDiff > 0) { lastCrossoverIdx = j; lastCrossoverType = 'bullish'; }
          else if (prevDiff > 0 && currDiff < 0) { lastCrossoverIdx = j; lastCrossoverType = 'bearish'; }
        }
        const lookbackBars = 3;
        let signal = 'none';
        let daysAgo = null;
        const currentEma21 = ema21[lastIdx];
        const currentEma55 = ema55[lastIdx];
        if (lastCrossoverIdx !== -1 && lastIdx - lastCrossoverIdx <= lookbackBars) {
          signal = lastCrossoverType;
          daysAgo = lastIdx - lastCrossoverIdx;
        }
        results.push({
          id: coin.id,
          name: coin.name,
          symbol: (coin.symbol || '').toUpperCase(),
          currentPrice: coin.current_price,
          priceChange24h: coin.price_change_percentage_24h,
          ema21: currentEma21,
          ema55: currentEma55,
          daysAgo,
          signal,
          timestamp: new Date().toISOString()
        });
        await this.sleep(this.isPro ? 800 : 2500);
      } catch (e) {
        console.warn(`EMA calc failed for ${coin?.id}:`, e.message);
      }
    }
    const bullCount = results.filter(r => r.signal === 'bullish').length;
    const bearCount = results.filter(r => r.signal === 'bearish').length;
    console.log(`✅ EMA crossovers — Bullish: ${bullCount}, Bearish: ${bearCount}, Total: ${results.length}`);
    return results;
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

      const options = { headers: requestHeaders, timeout: 20000 };

      const req = https.get(url, options, (res) => {
        let data = '';
        
        res.on('data', (chunk) => {
          data += chunk;
        });
        
        res.on('end', () => {
          const retryAfter = res.headers ? (res.headers['retry-after'] || res.headers['Retry-After']) : undefined;
          if (res.statusCode === 401 || res.statusCode === 403) {
            const err = new Error('Unauthorized - Check your API key');
            err.statusCode = res.statusCode;
            reject(err);
            return;
          }
          
          if (res.statusCode === 429) {
            const err = new Error('Rate limited - Too many requests');
            err.statusCode = 429;
            if (retryAfter) err.retryAfter = retryAfter;
            reject(err);
            return;
          }
          
          if (res.statusCode && res.statusCode >= 500) {
            const err = new Error(`Server error ${res.statusCode}`);
            err.statusCode = res.statusCode;
            reject(err);
            return;
          }
          
          if (res.statusCode !== 200) {
            const err = new Error(`HTTP ${res.statusCode}: ${data}`);
            err.statusCode = res.statusCode;
            reject(err);
            return;
          }

          try {
            const parsed = JSON.parse(data);
            resolve(parsed);
          } catch (error) {
            reject(new Error(`JSON parse error: ${error.message}`));
          }
        });
      });

      req.on('timeout', () => {
        req.destroy(new Error('Request timeout'));
      });

      req.on('error', (error) => {
        reject(error);
      });
    });
  }

  async makeAPICall(endpoint, params = {}) {
    const queryString = new URLSearchParams(params).toString();
    const buildUrl = () => `${this.BASE_URL}${endpoint}${queryString ? '?' + queryString : ''}`;
    const cacheKey = `${endpoint}|${queryString}`; // cache key decoupled from base URL
    
    const cached = this.cache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      console.log(`📦 Cache hit for: ${endpoint}`);
      return cached.data;
    }

    let attempt = 0;
    while (attempt <= this.maxRetries) {
      try {
        await this.enforceRateLimit();
        const url = buildUrl();
        console.log(`🌐 API call: ${endpoint} (attempt ${attempt + 1}/${this.maxRetries + 1})`);
        const data = await this.makeRequest(url);
        this.cache.set(cacheKey, { data, timestamp: Date.now() });
        return data;
      } catch (error) {
        const sc = error && error.statusCode;
        // Auto-downgrade from PRO to FREE on auth failure
        if ((sc === 401 || sc === 403) && this.isPro) {
          this.downgradeToFree();
          attempt++;
          continue;
        }
        // Retry on 429 / 5xx / transient network errors
        const transient = sc === 429 || (sc && sc >= 500) ||
          (error && ['ECONNRESET','ETIMEDOUT','EAI_AGAIN','ENOTFOUND'].includes(error.code));
        if (transient && attempt < this.maxRetries) {
          const delay = this.computeBackoffDelay(attempt, error && error.retryAfter);
          console.warn(`⏳ Retry ${attempt + 1} for ${endpoint} in ${delay}ms (reason: ${error.message})`);
          await this.sleep(delay);
          attempt++;
          continue;
        }

        console.error(`❌ API call failed: ${endpoint}`, error.message);
        if (cached) {
          console.log('📦 Using stale cache data');
          return cached.data;
        }
        throw error;
      }
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
        sparkline: this.isPro ? false : true,
        price_change_percentage: '24h,7d'
      });

      if (!Array.isArray(data)) {
        console.error('❌ Invalid data format from API:', data);
        continue;
      }

      allCoins.push(...data);
      
      // Add delay between requests to avoid rate limiting
      if (page < pages) {
        await this.sleep(this.isPro ? 400 : 2200);
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
      .map(c => ({ ...c, _7d: this.get7dChange(c) }))
      .filter(c => c._7d != null && c._7d > 0)
      .sort((a, b) => b._7d - a._7d)
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

  async getETHData() {
    console.log('Ξ Fetching ETH data...');
    try {
      const data = await this.makeAPICall('/coins/ethereum', {
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

      let trend = 'neutral';
      if (priceChange24h > 2) trend = 'strong_bull';
      else if (priceChange24h > 0.5) trend = 'bullish';
      else if (priceChange24h < -2) trend = 'strong_bear';
      else if (priceChange24h < -0.5) trend = 'bearish';

      console.log(`✅ ETH: $${currentPrice.toLocaleString()} (${priceChange24h > 0 ? '+' : ''}${priceChange24h.toFixed(2)}%), Trend: ${trend}`);

      return {
        currentPrice,
        priceChange24h,
        marketCap,
        trend
      };
    } catch (error) {
      console.error('⚠️ ETH data error:', error.message);
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
          per_page: 250,
          sparkline: this.isPro ? false : true
        });
        
        if (Array.isArray(response)) {
          allCoinData.push(...response);
        }
        
        // Add delay between batches
        await this.sleep(this.isPro ? 600 : 2000);
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
          price_change_percentage_7d: (this.get7dChange(coin) ?? 0),
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

  // Fallback computation for 7d change using sparkline when API omits price_change_percentage_7d_in_currency on FREE
  get7dChange(coin) {
    if (coin == null) return null;
    if (coin.price_change_percentage_7d_in_currency != null) return coin.price_change_percentage_7d_in_currency;
    const spark = coin.sparkline_in_7d && Array.isArray(coin.sparkline_in_7d.price) ? coin.sparkline_in_7d.price : null;
    if (!spark || spark.length < 2) return null;
    const first = spark[0];
    const last = spark[spark.length - 1];
    if (!first || first === 0) return null;
    return ((last - first) / first) * 100; // percent
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
      const ethData = await this.getETHData();
      const narrativeData = await this.getNarrativeData();
      // Compute technical analyses for cache consumers to avoid live API in serverless
      const emaCrossovers = await this.getEMACrossovers(topPerformers.allCoins);
      const corrFull = await this.getCorrelationAnalysis(topPerformers.allCoins);
      const topCorrelated = [...corrFull].sort((a, b) => b.correlation - a.correlation).slice(0, 10);
      const topDownsideBeta = [...corrFull].sort((a, b) => b.downsideBeta - a.downsideBeta).slice(0, 10);
      
      // Prepare complete data structure
      const dailyData = {
        date: getDateKeyNY(),
        timestamp: new Date().toISOString(),
        topGainers24h: topPerformers.topGainers24h,
        topLosers24h: topPerformers.topLosers24h,
        topGainers7d: topPerformers.topGainers7d,
        globalMetrics,
        btcData,
        ethData,
        narrativeData,
        emaCrossovers,
        correlationAnalysis: {
          topCorrelated,
          topDownsideBeta
        },
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
      console.log(`   - ETH price: $${dailyData.ethData.currentPrice?.toLocaleString()}`);
      console.log(`   - ETH price: $${ethData.currentPrice?.toLocaleString()}`);
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
