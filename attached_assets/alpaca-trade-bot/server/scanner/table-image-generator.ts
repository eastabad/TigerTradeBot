/**
 * Table Image Generator
 * Generates scan result tables as images for Discord
 */

import { createCanvas, registerFont } from "canvas";
import { SignalStateChange, AuxiliaryMatchInfo } from "./strategy-engine";
import * as fs from "fs";
import * as path from "path";

const FONT_FAMILY = "WenQuanYi Micro Hei, DejaVu Sans, Arial, sans-serif";

const STRATEGY_TRANSLATIONS: Record<string, string> = {
  // 5Min strategies
  "5Min入场-极强趋势做多": "5M-SuperStrong Long",
  "5Min入场-极强趋势做空": "5M-SuperStrong Short",
  "5Min入场-强趋势做多": "5M-Strong Long",
  "5Min入场-强趋势做空": "5M-Strong Short",
  "5Min入场-趋势共振做多": "5M-Resonance Long",
  "5Min入场-趋势共振做空": "5M-Resonance Short",
  // 15Min strategies
  "15Min入场-极强趋势做多": "15M-SuperStrong Long",
  "15Min入场-极强趋势做空": "15M-SuperStrong Short",
  "15Min入场-强趋势做多": "15M-Strong Long",
  "15Min入场-强趋势做空": "15M-Strong Short",
  "15Min入场-趋势共振做多": "15M-Resonance Long",
  "15Min入场-趋势共振做空": "15M-Resonance Short",
  // 1Hour strategies
  "1H入场-趋势做空": "1H-Trend Short",
  "1H入场-趋势做多": "1H-Trend Long",
  "1H入场-底部反弹A": "1H-Bottom Rebound A",
  "1H入场-底部反弹B": "1H-Bottom Rebound B",
  "1H入场-顶部反转A": "1H-Top Reversal A",
  "1H入场-顶部反转B": "1H-Top Reversal B",
  // 4Hour strategies
  "4H入场-趋势做空": "4H-Trend Short",
  "4H入场-趋势做多": "4H-Trend Long",
  "4H入场-底部反弹": "4H-Bottom Rebound",
  "4H入场-顶部反转": "4H-Top Reversal",
};

function translateStrategy(name: string): string {
  return STRATEGY_TRANSLATIONS[name] || name.replace(/[\u4e00-\u9fff]/g, "");
}

const TIMEFRAMES = ["5Min", "15Min", "1Hour", "4Hour"];
const TF_LABELS: Record<string, string> = {
  "5Min": "5Min",
  "15Min": "15Min",
  "1Hour": "1Hour",
  "4Hour": "4Hour",
};

const COLORS = {
  background: "#1a1a2e",
  headerBg: "#16213e",
  rowBg1: "#1a1a2e",
  rowBg2: "#0f0f23",
  scanColumnBg: "#1e3a5f",
  border: "#2d3748",
  headerText: "#e2e8f0",
  symbolText: "#ffffff",
  priceText: "#a0aec0",
  longText: "#22c55e",
  shortText: "#ef4444",
  emptyText: "#4a5568",
  titleBg: "#0d1b2a",
  titleText: "#60a5fa",
};

interface TableData {
  symbol: string;
  price?: number;
  strategies: Record<string, string[]>;
  signalType: "LONG" | "SHORT";
}

function formatPrice(price: number): string {
  return `$${price.toFixed(2)}`;
}

function buildTableData(
  scanTimeframe: string,
  currentMatches: SignalStateChange[],
  auxiliaryMatches: AuxiliaryMatchInfo[],
  signalType: "LONG" | "SHORT"
): TableData[] {
  const symbolData: Map<string, TableData> = new Map();

  for (const entry of currentMatches) {
    if (entry.signalType !== signalType) continue;
    
    if (!symbolData.has(entry.symbol)) {
      symbolData.set(entry.symbol, {
        symbol: entry.symbol,
        price: entry.price,
        strategies: {},
        signalType,
      });
    }
    
    const data = symbolData.get(entry.symbol)!;
    if (!data.strategies[scanTimeframe]) {
      data.strategies[scanTimeframe] = [];
    }
    data.strategies[scanTimeframe].push(entry.strategyName);
    if (entry.price && !data.price) {
      data.price = entry.price;
    }
  }

  for (const aux of auxiliaryMatches) {
    for (const match of aux.matches) {
      if (match.signalType !== signalType) continue;
      
      if (!symbolData.has(match.symbol)) {
        symbolData.set(match.symbol, {
          symbol: match.symbol,
          strategies: {},
          signalType,
        });
      }
      
      const data = symbolData.get(match.symbol)!;
      if (!data.strategies[aux.timeframe]) {
        data.strategies[aux.timeframe] = [];
      }
      data.strategies[aux.timeframe].push(match.strategyName);
    }
  }

  return Array.from(symbolData.values()).sort((a, b) => a.symbol.localeCompare(b.symbol));
}

export function generateTableImage(
  scanTimeframe: string,
  currentMatches: SignalStateChange[],
  auxiliaryMatches: AuxiliaryMatchInfo[],
  signalType: "LONG" | "SHORT"
): Buffer | null {
  const tableData = buildTableData(scanTimeframe, currentMatches, auxiliaryMatches, signalType);
  
  if (tableData.length === 0) {
    return null;
  }

  const padding = 15;
  const headerHeight = 50;
  const titleHeight = 45;
  const rowHeight = 55;
  const symbolColWidth = 100;
  const tfColWidth = 180;
  const totalWidth = symbolColWidth + tfColWidth * 4 + padding * 2;
  const totalHeight = titleHeight + headerHeight + rowHeight * Math.min(tableData.length, 25) + padding * 2 + 35;

  const canvas = createCanvas(totalWidth, totalHeight);
  const ctx = canvas.getContext("2d");

  ctx.fillStyle = COLORS.background;
  ctx.fillRect(0, 0, totalWidth, totalHeight);

  const typeLabel = signalType === "LONG" ? "LONG" : "SHORT";
  const typeEmoji = signalType === "LONG" ? "[LONG]" : "[SHORT]";
  const scanLabel = TF_LABELS[scanTimeframe] || scanTimeframe;
  const timestamp = new Date().toLocaleString("en-US", { 
    timeZone: "America/New_York",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });

  ctx.fillStyle = COLORS.titleBg;
  ctx.fillRect(0, 0, totalWidth, titleHeight);
  
  ctx.fillStyle = COLORS.titleText;
  ctx.font = `bold 18px ${FONT_FAMILY}`;
  ctx.textAlign = "center";
  ctx.fillText(
    `${typeEmoji} ${scanLabel} TD AIScaner - ${typeLabel} | ${timestamp} ET`,
    totalWidth / 2,
    titleHeight / 2 + 6
  );

  const headerY = titleHeight;
  ctx.fillStyle = COLORS.headerBg;
  ctx.fillRect(0, headerY, totalWidth, headerHeight);

  ctx.strokeStyle = COLORS.border;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, headerY + headerHeight);
  ctx.lineTo(totalWidth, headerY + headerHeight);
  ctx.stroke();

  ctx.fillStyle = COLORS.headerText;
  ctx.font = `bold 14px ${FONT_FAMILY}`;
  ctx.textAlign = "center";

  const headers = ["Symbol", "5Min", "15Min", "1Hour", "4Hour"];
  let xPos = padding + symbolColWidth / 2;
  ctx.fillText(headers[0], xPos, headerY + headerHeight / 2 + 5);
  
  xPos = padding + symbolColWidth;
  for (let i = 1; i < headers.length; i++) {
    const tf = TIMEFRAMES[i - 1];
    const colX = xPos + (i - 1) * tfColWidth;
    
    if (tf === scanTimeframe) {
      ctx.fillStyle = COLORS.scanColumnBg;
      ctx.fillRect(colX, headerY, tfColWidth, headerHeight);
      ctx.fillStyle = COLORS.titleText;
    } else {
      ctx.fillStyle = COLORS.headerText;
    }
    
    ctx.fillText(headers[i], colX + tfColWidth / 2, headerY + headerHeight / 2 + 5);
  }

  const dataStartY = headerY + headerHeight;
  const displayData = tableData.slice(0, 25);

  for (let rowIdx = 0; rowIdx < displayData.length; rowIdx++) {
    const row = displayData[rowIdx];
    const rowY = dataStartY + rowIdx * rowHeight;

    ctx.fillStyle = rowIdx % 2 === 0 ? COLORS.rowBg1 : COLORS.rowBg2;
    ctx.fillRect(0, rowY, totalWidth, rowHeight);

    xPos = padding + symbolColWidth;
    for (let i = 0; i < TIMEFRAMES.length; i++) {
      const tf = TIMEFRAMES[i];
      const colX = xPos + i * tfColWidth;
      
      if (tf === scanTimeframe) {
        ctx.fillStyle = COLORS.scanColumnBg + "40";
        ctx.fillRect(colX, rowY, tfColWidth, rowHeight);
      }
    }

    ctx.strokeStyle = COLORS.border;
    ctx.beginPath();
    ctx.moveTo(0, rowY + rowHeight);
    ctx.lineTo(totalWidth, rowY + rowHeight);
    ctx.stroke();

    ctx.fillStyle = COLORS.symbolText;
    ctx.font = `bold 14px ${FONT_FAMILY}`;
    ctx.textAlign = "left";
    ctx.fillText(row.symbol, padding + 10, rowY + 22);
    
    if (row.price) {
      ctx.fillStyle = COLORS.priceText;
      ctx.font = `12px ${FONT_FAMILY}`;
      ctx.fillText(formatPrice(row.price), padding + 10, rowY + 40);
    }

    xPos = padding + symbolColWidth;
    for (let i = 0; i < TIMEFRAMES.length; i++) {
      const tf = TIMEFRAMES[i];
      const colX = xPos + i * tfColWidth;
      const strategies = row.strategies[tf];
      
      if (strategies && strategies.length > 0) {
        ctx.fillStyle = signalType === "LONG" ? COLORS.longText : COLORS.shortText;
        ctx.font = `12px ${FONT_FAMILY}`;
        ctx.textAlign = "left";
        
        const maxStrategies = 2;
        for (let j = 0; j < Math.min(strategies.length, maxStrategies); j++) {
          const translated = translateStrategy(strategies[j]);
          const strategyText = translated.length > 20 
            ? translated.substring(0, 18) + "..."
            : translated;
          ctx.fillText(strategyText, colX + 8, rowY + 20 + j * 16);
        }
        
        if (strategies.length > maxStrategies) {
          ctx.fillStyle = COLORS.priceText;
          ctx.fillText(`+${strategies.length - maxStrategies}more`, colX + 8, rowY + 20 + maxStrategies * 16);
        }
      } else {
        ctx.fillStyle = COLORS.emptyText;
        ctx.font = `14px ${FONT_FAMILY}`;
        ctx.textAlign = "center";
        ctx.fillText("-", colX + tfColWidth / 2, rowY + rowHeight / 2 + 5);
      }
    }
  }

  let currentX = padding + symbolColWidth;
  for (let i = 0; i <= TIMEFRAMES.length; i++) {
    ctx.strokeStyle = COLORS.border;
    ctx.beginPath();
    ctx.moveTo(currentX, headerY);
    ctx.lineTo(currentX, dataStartY + displayData.length * rowHeight);
    ctx.stroke();
    currentX += tfColWidth;
  }

  const footerY = dataStartY + displayData.length * rowHeight + 10;
  ctx.fillStyle = COLORS.priceText;
  ctx.font = `12px ${FONT_FAMILY}`;
  ctx.textAlign = "center";
  
  let footerText = `Total: ${tableData.length} stocks`;
  if (tableData.length > 25) {
    footerText += ` (showing top 25)`;
  }
  footerText += ` | Scan: ${scanLabel} (highlighted)`;
  ctx.fillText(footerText, totalWidth / 2, footerY + 10);

  return canvas.toBuffer("image/png");
}

export function generateBothTables(
  scanTimeframe: string,
  currentMatches: SignalStateChange[],
  auxiliaryMatches: AuxiliaryMatchInfo[]
): { long: Buffer | null; short: Buffer | null } {
  const longImage = generateTableImage(scanTimeframe, currentMatches, auxiliaryMatches, "LONG");
  const shortImage = generateTableImage(scanTimeframe, currentMatches, auxiliaryMatches, "SHORT");
  
  return { long: longImage, short: shortImage };
}
