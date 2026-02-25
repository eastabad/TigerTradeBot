/**
 * Technical Indicators Module
 * Contains: HeikinAshiTSI, WeightedQQE, SincMomentum
 */

export interface BarData {
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  timestamp: Date;
}

// ==================== Helper Functions ====================

function ema(source: number[], length: number): number[] {
  // Matches TradingView: ewm(alpha=2/(n+1), adjust=False, min_periods=length).mean()
  const alpha = 2.0 / (length + 1);
  const result: number[] = new Array(source.length).fill(NaN);
  
  let emaValue = NaN;
  let validCount = 0;
  
  for (let i = 0; i < source.length; i++) {
    const val = source[i];
    if (!isNaN(val)) {
      validCount++;
      if (isNaN(emaValue)) {
        emaValue = val; // seed with first valid value
      } else {
        emaValue = alpha * val + (1 - alpha) * emaValue;
      }
      // Only output when we have min_periods (length) valid values
      if (validCount >= length) {
        result[i] = emaValue;
      }
    }
  }
  
  return result;
}

function wma(source: number[], length: number): number[] {
  const result: number[] = new Array(source.length).fill(NaN);
  const weights = Array.from({ length }, (_, i) => i + 1);
  const weightSum = weights.reduce((a, b) => a + b, 0);

  for (let i = length - 1; i < source.length; i++) {
    let sum = 0;
    for (let j = 0; j < length; j++) {
      sum += source[i - length + 1 + j] * weights[j];
    }
    result[i] = sum / weightSum;
  }
  return result;
}

// RMA matching TradingView ta.rma: ewm(alpha=1/length, adjust=False, min_periods=length)
function rma(source: number[], length: number): number[] {
  const alpha = 1.0 / length;
  const n = source.length;
  const result: number[] = new Array(n).fill(NaN);
  
  // Find first valid value and accumulate for warmup
  let validCount = 0;
  let sum = 0;
  let startIdx = -1;
  
  for (let i = 0; i < n; i++) {
    if (!isNaN(source[i])) {
      sum += source[i];
      validCount++;
      if (validCount === length) {
        startIdx = i;
        result[i] = sum / length; // Initialize with SMA for first value
        break;
      }
    }
  }
  
  if (startIdx === -1) return result;
  
  // Continue with EWM calculation
  for (let i = startIdx + 1; i < n; i++) {
    if (!isNaN(source[i])) {
      result[i] = alpha * source[i] + (1 - alpha) * result[i - 1];
    } else {
      result[i] = result[i - 1]; // Carry forward
    }
  }
  
  return result;
}

function diff(source: number[]): number[] {
  // TradingView ta.change returns na for first bar
  const result: number[] = new Array(source.length).fill(NaN);
  for (let i = 1; i < source.length; i++) {
    result[i] = source[i] - source[i - 1];
  }
  return result;
}

function abs(source: number[]): number[] {
  return source.map(v => isNaN(v) ? NaN : Math.abs(v));
}

// ==================== Heikin Ashi TSI ====================

export interface TSIParams {
  slowLength?: number;
  fastLength?: number;
  signalLength?: number;
  maType?: "EMA" | "WMA";
  haLength?: number;
  obLevel?: number;
  osLevel?: number;
}

export interface TSIResult {
  tsiClose: number;
  tsiSignal: number;
  haOpen: number;
  haClose: number;
  directionState: number; // 1=bull, -1=bear, 0=neutral
  heikinBullish: boolean; // HA candle is green
  heikinBearish: boolean; // HA candle is red
  maBullish: boolean; // TSI MA direction is bullish
  maBearish: boolean; // TSI MA direction is bearish
  isBullish: boolean; // Combined: HA bullish AND MA bullish
  isBearish: boolean; // Combined: HA bearish AND MA bearish
  isOverbought: boolean;
  isOversold: boolean;
}

export function calculateTSI(bars: BarData[], params: TSIParams = {}): TSIResult {
  const {
    slowLength = 35,      // Swing Trading mode
    fastLength = 21,      // Swing Trading mode
    signalLength = 14,    // Swing Trading mode
    maType = "EMA",
    haLength = 1,
    obLevel = 35,
    osLevel = -35,
  } = params;

  const n = bars.length;
  const minRequired = slowLength + fastLength + signalLength * 2 + 10; // Extra warmup buffer
  if (n < minRequired) {
    return {
      tsiClose: 0, tsiSignal: 0, haOpen: 0, haClose: 0,
      directionState: 0, 
      heikinBullish: false, heikinBearish: false,
      maBullish: false, maBearish: false,
      isBullish: false, isBearish: false,
      isOverbought: false, isOversold: false,
    };
  }

  const maFunc = maType === "EMA" ? ema : wma;

  // Use OHLC4 as source (matching TradingView: srcTSI = (o+h+l+c)/4)
  const ohlc4 = bars.map(b => (b.open + b.high + b.low + b.close) / 4);
  const highs = bars.map(b => b.high);
  const lows = bars.map(b => b.low);

  // TSI calculation function
  const calcTSI = (prices: number[]): number[] => {
    const pc = diff(prices);
    const firstSmooth = maFunc(pc, slowLength);
    const doubleSmoothedPc = maFunc(firstSmooth, fastLength);
    const firstSmoothAbs = maFunc(abs(pc), slowLength);
    const doubleSmoothedAbsPc = maFunc(firstSmoothAbs, fastLength);
    
    return doubleSmoothedPc.map((v, i) => {
      const denom = doubleSmoothedAbsPc[i];
      if (isNaN(v) || isNaN(denom) || denom === 0) return NaN;
      return 100 * (v / denom);
    });
  };

  const tsiClose = calcTSI(ohlc4);
  const tsiHigh = calcTSI(highs);
  const tsiLow = calcTSI(lows);

  // Signal line (single MA, matching TradingView)
  const tsiSignal = maFunc(tsiClose, signalLength);

  // Heikin Ashi calculation
  const haClose = tsiClose.slice();
  const haOpen: number[] = new Array(n).fill(0);
  haOpen[0] = (haClose[0] + haClose[0]) / 2;
  for (let i = 1; i < n; i++) {
    const prevIdx = Math.max(0, i - haLength);
    haOpen[i] = (haOpen[prevIdx] + haClose[prevIdx]) / 2;
  }

  // Direction state
  const directionState: number[] = new Array(n).fill(0);
  for (let i = 1; i < n; i++) {
    if (tsiHigh[i] > tsiSignal[i] && tsiLow[i] > tsiSignal[i]) {
      directionState[i] = 1;
    } else if (tsiHigh[i] < tsiSignal[i] && tsiLow[i] < tsiSignal[i]) {
      directionState[i] = -1;
    } else {
      directionState[i] = directionState[i - 1];
    }
  }

  // Latest values - find last non-NaN value
  const last = n - 1;
  const tsiVal = isNaN(tsiClose[last]) ? 0 : tsiClose[last];
  const sigVal = isNaN(tsiSignal[last]) ? 0 : tsiSignal[last];
  const haOVal = isNaN(haOpen[last]) ? 0 : haOpen[last];
  const haCVal = isNaN(haClose[last]) ? tsiVal : haClose[last];
  
  const heikinBullish = haCVal > haOVal;
  const heikinBearish = haCVal < haOVal;
  const maBullish = directionState[last] === 1;
  const maBearish = directionState[last] === -1;

  return {
    tsiClose: tsiVal,
    tsiSignal: sigVal,
    haOpen: haOVal,
    haClose: haCVal,
    directionState: directionState[last],
    heikinBullish,
    heikinBearish,
    maBullish,
    maBearish,
    isBullish: heikinBullish && maBullish,
    isBearish: heikinBearish && maBearish,
    isOverbought: tsiVal > obLevel,
    isOversold: tsiVal < osLevel,
  };
}

// ==================== Weighted QQE ====================

export interface QQEParams {
  factor?: number;
  weight?: number;
  stLength?: number;
  stSmooth?: number;
  ltLength?: number;
  ltSmooth?: number;
  overbought?: number;
  oversold?: number;
}

// QQE状态定义（基于TradingView配色方案）
// state: "bullish_aligned" | "pullback" | "rebound" | "bearish_aligned"
export interface QQEResult {
  rsiSt: number;           // 短周期RSI值
  tsSt: number;            // 短周期趋势线
  rsiLt: number;           // 长周期RSI值
  tsLt: number;            // 长周期趋势线
  stBull: boolean;         // 短周期看涨
  ltBull: boolean;         // 长周期看涨
  // 4种核心状态（基于配色方案）
  state: "bullish_aligned" | "pullback" | "rebound" | "bearish_aligned";
  // 过滤条件
  isOverbought: boolean;   // rsiSt > 70 (过热警告)
  isOversold: boolean;     // rsiSt < 30 (超跌机会)
}

export function calculateQQE(bars: BarData[], params: QQEParams = {}): QQEResult {
  const {
    factor = 4.236,
    weight = 2.0,
    stLength = 14,
    stSmooth = 5,
    ltLength = 34,
    ltSmooth = 14,
    overbought = 70,
    oversold = 30,
  } = params;

  const n = bars.length;
  const minRequired = Math.max(stLength, ltLength) + Math.max(stSmooth, ltSmooth) * 2 + 20; // Extra warmup buffer
  if (n < minRequired) {
    return {
      rsiSt: 50, tsSt: 50, rsiLt: 50, tsLt: 50,
      stBull: false, ltBull: false, state: "bearish_aligned" as const,
      isOverbought: false, isOversold: false,
    };
  }

  const closes = bars.map(b => b.close);

  // 完全对齐TradingView Python版本的WQQE计算
  const calcWQQE = (rmaLength: number, emaSmooth: number): { rsi: number[]; ts: number[] } => {
    const delta = diff(closes);
    
    // Step 1: 计算加权的num和den（需要前一个bar的rsi和ts来计算权重w）
    const wqqeRsi: number[] = new Array(n).fill(0);
    const wqqeTs: number[] = new Array(n).fill(0);
    const num: number[] = new Array(n).fill(NaN);
    const den: number[] = new Array(n).fill(NaN);
    
    // 第一遍：计算num和den（带权重）
    for (let i = 1; i < n; i++) {
      const prevRsi = wqqeRsi[i - 1];
      const prevTs = wqqeTs[i - 1];
      const w = delta[i] * (prevRsi - prevTs) > 0 ? weight : 1.0;
      
      num[i] = delta[i] * w;
      den[i] = Math.abs(delta[i] * w);
    }
    
    // Step 2: 对num和den做RMA
    const numRma = rma(num, rmaLength);
    const denRma = rma(den, rmaLength);
    
    // Step 3: 计算rsi_raw = num_rma / den_rma
    const rsiRaw: number[] = new Array(n).fill(NaN);
    for (let i = 0; i < n; i++) {
      if (!isNaN(numRma[i]) && !isNaN(denRma[i]) && denRma[i] !== 0) {
        rsiRaw[i] = numRma[i] / denRma[i];
      }
    }
    
    // Step 4: 对rsi_raw做EMA，然后 wqqe_rsi = 50 * ema(rsi_raw) + 50
    const rsiEma = ema(rsiRaw, emaSmooth);
    for (let i = 0; i < n; i++) {
      wqqeRsi[i] = !isNaN(rsiEma[i]) ? 50 * rsiEma[i] + 50 : 50;
    }
    
    // Step 5: 计算diff = rma(abs(wqqe_rsi - wqqe_rsi.shift(1)), length)
    const rsiChange: number[] = new Array(n).fill(NaN);
    for (let i = 1; i < n; i++) {
      rsiChange[i] = Math.abs(wqqeRsi[i] - wqqeRsi[i - 1]);
    }
    const diffArr = rma(rsiChange, rmaLength);
    
    // Step 6: 第二个循环计算trailing stop (wqqe_ts)
    for (let i = 1; i < n; i++) {
      const prevTs = wqqeTs[i - 1];
      const currRsi = wqqeRsi[i];
      const prevRsi = wqqeRsi[i - 1];
      const dif = !isNaN(diffArr[i]) ? diffArr[i] : 0;
      
      // crossover: currRsi > prevTs && prevRsi <= prevTs
      // crossunder: currRsi < prevTs && prevRsi >= prevTs
      const crossover = currRsi > prevTs && prevRsi <= prevTs;
      const crossunder = currRsi < prevTs && prevRsi >= prevTs;
      
      if (crossover) {
        wqqeTs[i] = currRsi - dif * factor;
      } else if (crossunder) {
        wqqeTs[i] = currRsi + dif * factor;
      } else if (currRsi > prevTs) {
        wqqeTs[i] = Math.max(currRsi - dif * factor, prevTs);
      } else {
        wqqeTs[i] = Math.min(currRsi + dif * factor, prevTs);
      }
    }
    
    return { rsi: wqqeRsi, ts: wqqeTs };
  };

  const st = calcWQQE(stLength, stSmooth);
  const lt = calcWQQE(ltLength, ltSmooth);

  const last = n - 1;
  const prev = n - 2;

  const stBull = st.rsi[last] > st.ts[last];
  const ltBull = lt.rsi[last] > lt.ts[last];

  // 根据TradingView配色方案定义4种状态
  // bullish_aligned: 多头共振（亮绿/过热时青色）
  // pullback: 回调（长周期看涨但短周期回落）
  // rebound: 反弹（长周期看跌但短周期反弹）
  // bearish_aligned: 空头共振（深红/超跌时粉红）
  let state: "bullish_aligned" | "pullback" | "rebound" | "bearish_aligned";
  if (ltBull && stBull) state = "bullish_aligned";
  else if (ltBull && !stBull) state = "pullback";
  else if (!ltBull && stBull) state = "rebound";
  else state = "bearish_aligned";

  return {
    rsiSt: st.rsi[last],
    tsSt: st.ts[last],
    rsiLt: lt.rsi[last],
    tsLt: lt.ts[last],
    stBull,
    ltBull,
    state,
    // 过滤条件
    isOverbought: st.rsi[last] > overbought,  // 过热警告
    isOversold: st.rsi[last] < oversold,      // 超跌机会
  };
}

// ==================== Sinc Momentum ====================

export interface MomentumParams {
  momLength?: number;
  momMaLength?: number;
  swLength?: number;
  swMaLength?: number;
  lsLength?: number;
  lsMaLength?: number;
  postSmoothing?: number;
}

export interface MomentumResult {
  stateMom: number;
  stateSw: number;
  stateLs: number;
  composite: number; // -4 to +4
  momoMom: number;
  momoSw: number;
  momoLs: number;
}

function sinc(x: number): number {
  if (x === 0) return 1;
  return Math.sin(Math.PI * x) / (Math.PI * x);
}

function blackman(n: number, length: number): number {
  return 0.42 - 0.5 * Math.cos((2 * Math.PI * n) / (length - 1)) 
       + 0.08 * Math.cos((4 * Math.PI * n) / (length - 1));
}

function sincCoefficients(length: number, fc: number): number[] {
  const coefficients: number[] = [];
  const mid = Math.floor((length - 1) / 2);
  const cutoff = 1.0 / fc;
  const isEven = length % 2 === 0;

  for (let i = 0; i < length; i++) {
    const n = i - mid;
    const k = i;
    const coef = sinc(2 * cutoff * n) * blackman(isEven ? k + 0.5 : k, length);
    coefficients.push(coef);
  }
  return coefficients;
}

// LTI Sinc filter matching Python: if i > length (not i >= length - 1)
function ltiSinc(data: number[], length: number, fc: number): number[] {
  const coefficients = sincCoefficients(length, fc);
  const normalize = coefficients.reduce((a, b) => a + b, 0);
  const result: number[] = new Array(data.length).fill(NaN);

  // Python uses: if i > length, which means i >= length + 1
  // data window: source.iloc[i - length + 1:i + 1].values[::-1] (newest first)
  for (let i = length + 1; i < data.length; i++) {
    // Get window [i - length + 1, i] reversed (newest first like Pine array.unshift)
    let sum = 0;
    for (let j = 0; j < length; j++) {
      // coefficients[j] * data[i - j] (newest first)
      sum += data[i - j] * coefficients[j];
    }
    result[i] = sum / normalize;
  }
  return result;
}

function filterMA(source: number[], length: number, style: "WMA" | "EMA"): number[] {
  if (length <= 1) return source.slice();
  return style === "WMA" ? wma(source, length) : ema(source, length);
}

export function calculateMomentum(bars: BarData[], params: MomentumParams = {}): MomentumResult {
  const {
    momLength = 50,
    momMaLength = 25,
    swLength = 100,
    swMaLength = 50,
    lsLength = 200,
    lsMaLength = 100,
    postSmoothing = 5,
  } = params;

  const n = bars.length;
  const minRequired = lsLength * 2 + lsMaLength + postSmoothing;

  if (n < minRequired) {
    return {
      stateMom: 0, stateSw: 0, stateLs: 0, composite: 0,
      momoMom: 0, momoSw: 0, momoLs: 0,
    };
  }

  const closes = bars.map(b => b.close);

  const calcMomoSet = (momLen: number, maLen: number): { delta: number[]; momo: number[]; state: number[] } => {
    const lenCorr = momLen * 2;
    const offVal = Math.floor((lenCorr - 1) / 2);

    const sincVal = ltiSinc(closes, lenCorr, momLen);
    const deltaRaw: number[] = closes.map((c, i) => {
      if (isNaN(sincVal[i])) return NaN;
      return (c - sincVal[i]) / offVal;
    });

    const delta = filterMA(deltaRaw, postSmoothing, "WMA");
    const maTemp = filterMA(delta, 2, "EMA");
    const ma = filterMA(maTemp, maLen, "EMA");
    const momo = delta.map((d, i) => d - ma[i]);

    // Calculate state
    const state: number[] = new Array(n).fill(0);
    for (let i = 1; i < n; i++) {
      if (isNaN(momo[i]) || isNaN(delta[i])) continue;
      
      const m = momo[i];
      const d = delta[i];
      const dPrev = delta[i - 1];

      const isBull = m > 0;
      const isBullPlus = m > 0 && (d > dPrev || d > 0);
      const isBear = m < 0;
      const isBearPlus = m < 0 && (d < dPrev || d < 0);

      if (isBullPlus) state[i] = 2;
      else if (isBull) state[i] = 1;
      else if (isBearPlus) state[i] = -2;
      else if (isBear) state[i] = -1;
    }

    return { delta, momo, state };
  };

  const mom = calcMomoSet(momLength, momMaLength);
  const sw = calcMomoSet(swLength, swMaLength);
  const ls = calcMomoSet(lsLength, lsMaLength);

  const last = n - 1;
  const composite = sw.state[last] + ls.state[last];

  return {
    stateMom: mom.state[last],
    stateSw: sw.state[last],
    stateLs: ls.state[last],
    composite,
    momoMom: mom.momo[last] || 0,
    momoSw: sw.momo[last] || 0,
    momoLs: ls.momo[last] || 0,
  };
}

// ==================== Combined Indicator Calculator ====================

export interface AllIndicatorResults {
  tsi: TSIResult;
  qqe: QQEResult;
  momentum: MomentumResult;
}

export function calculateAllIndicators(
  bars: BarData[],
  tsiParams?: TSIParams,
  qqeParams?: QQEParams,
  momentumParams?: MomentumParams
): AllIndicatorResults {
  return {
    tsi: calculateTSI(bars, tsiParams),
    qqe: calculateQQE(bars, qqeParams),
    momentum: calculateMomentum(bars, momentumParams),
  };
}
