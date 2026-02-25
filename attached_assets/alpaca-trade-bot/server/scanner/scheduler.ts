import cron, { ScheduledTask } from "node-cron";
import * as scanner from "./scanner-core";
import { runAllActiveStrategiesWithTracking, SignalStateChange, AuxiliaryMatchInfo } from "./strategy-engine";
import { sendScanResultsToDiscord } from "./discord-notifier";

interface SchedulerState {
  isRunning: boolean;
  lastRun: Record<string, Date | null>;
  nextRun: Record<string, Date | null>;
  results: Record<string, { matches: number; newEntries: number; timestamp: Date }>;
  errors: Record<string, string>;
  latestNewEntries: SignalStateChange[];
  // Auxiliary matches from other timeframes for the most recent scan
  latestAuxiliaryMatches: AuxiliaryMatchInfo[];
}

const state: SchedulerState = {
  isRunning: false,
  lastRun: {
    "5Min": null,
    "15Min": null,
    "1Hour": null,
    "4Hour": null,
  },
  nextRun: {
    "5Min": null,
    "15Min": null,
    "1Hour": null,
    "4Hour": null,
  },
  results: {},
  errors: {},
  latestNewEntries: [],
  latestAuxiliaryMatches: [],
};

const scheduledTasks: Record<string, ScheduledTask> = {};

function updateNextRun(timeframe: string): void {
  const now = new Date();
  const intervals: Record<string, number> = {
    "5Min": 5 * 60 * 1000,
    "15Min": 15 * 60 * 1000,
    "1Hour": 60 * 60 * 1000,
    "4Hour": 4 * 60 * 60 * 1000,
  };
  const interval = intervals[timeframe] || 15 * 60 * 1000;
  state.nextRun[timeframe] = new Date(Math.ceil(now.getTime() / interval) * interval);
}

async function runTimeframeScan(timeframe: string): Promise<void> {
  console.log(`[Scheduler] Starting ${timeframe} scan at ${new Date().toISOString()}`);
  
  try {
    const watchlist = await scanner.getActiveWatchlist();
    console.log(`[Scheduler] Scanning ${watchlist.length} symbols for ${timeframe}`);
    
    let processedCount = 0;
    let errorCount = 0;
    
    for (const symbol of watchlist) {
      try {
        await scanner.scanSymbol(symbol, timeframe, true);
        processedCount++;
      } catch (error) {
        errorCount++;
        console.error(`[Scheduler] Error scanning ${symbol} for ${timeframe}:`, error);
      }
    }
    
    console.log(`[Scheduler] ${timeframe} data update complete: ${processedCount} success, ${errorCount} errors`);
    
    const trackingResult = await runAllActiveStrategiesWithTracking(timeframe);
    
    let totalMatches = 0;
    trackingResult.allMatches.forEach((r) => {
      totalMatches += r.matches?.length || 0;
    });
    
    state.lastRun[timeframe] = new Date();
    state.results[timeframe] = {
      matches: totalMatches,
      newEntries: trackingResult.newEntries.length,
      timestamp: new Date(),
    };
    
    if (trackingResult.newEntries.length > 0) {
      state.latestNewEntries = [...trackingResult.newEntries, ...state.latestNewEntries].slice(0, 20);
    }
    
    // Store auxiliary matches from other timeframes (for decision support)
    state.latestAuxiliaryMatches = trackingResult.auxiliaryMatches;
    
    delete state.errors[timeframe];
    
    updateNextRun(timeframe);
    
    const auxInfo = trackingResult.auxiliaryMatches.map(a => `${a.timeframe}:${a.matches.length}`).join(", ");
    console.log(`[Scheduler] ${timeframe} scan complete. Matches: ${totalMatches}, New entries: ${trackingResult.newEntries.length}, Exits: ${trackingResult.exits.length}, Aux: [${auxInfo}]`);
    
    // Send results to Discord (only NEW entries for current timeframe, not continuing)
    await sendScanResultsToDiscord(
      timeframe,
      trackingResult.newEntries,
      trackingResult.auxiliaryMatches
    );
    
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    state.errors[timeframe] = errorMsg;
    console.error(`[Scheduler] ${timeframe} scan failed:`, errorMsg);
  }
}

export function startScheduler(): void {
  if (state.isRunning) {
    console.log("[Scheduler] Already running");
    return;
  }

  console.log("[Scheduler] Starting automated scanner scheduler...");

  scheduledTasks["5Min"] = cron.schedule("*/5 * * * *", async () => {
    if (isMarketHours()) {
      await runTimeframeScan("5Min");
    }
  });

  scheduledTasks["15Min"] = cron.schedule("*/15 * * * *", async () => {
    if (isMarketHours()) {
      await runTimeframeScan("15Min");
    }
  });

  scheduledTasks["1Hour"] = cron.schedule("0 * * * *", async () => {
    if (isMarketHours()) {
      await runTimeframeScan("1Hour");
    }
  });

  scheduledTasks["4Hour"] = cron.schedule("0 */4 * * *", async () => {
    if (isMarketHours()) {
      await runTimeframeScan("4Hour");
    }
  });

  state.isRunning = true;
  
  const now = new Date();
  state.nextRun["5Min"] = new Date(Math.ceil(now.getTime() / (5 * 60 * 1000)) * (5 * 60 * 1000));
  state.nextRun["15Min"] = new Date(Math.ceil(now.getTime() / (15 * 60 * 1000)) * (15 * 60 * 1000));
  state.nextRun["1Hour"] = new Date(Math.ceil(now.getTime() / (60 * 60 * 1000)) * (60 * 60 * 1000));
  state.nextRun["4Hour"] = new Date(Math.ceil(now.getTime() / (4 * 60 * 60 * 1000)) * (4 * 60 * 60 * 1000));

  console.log("[Scheduler] Scheduler started with schedules:");
  console.log("  - 5Min:  every 5 minutes");
  console.log("  - 15Min: every 15 minutes");
  console.log("  - 1Hour: every hour at :00");
  console.log("  - 4Hour: every 4 hours at :00");
}

export function stopScheduler(): void {
  if (!state.isRunning) {
    console.log("[Scheduler] Not running");
    return;
  }

  Object.values(scheduledTasks).forEach(task => task.stop());
  Object.keys(scheduledTasks).forEach(key => delete scheduledTasks[key]);
  
  state.isRunning = false;
  state.nextRun = {
    "5Min": null,
    "15Min": null,
    "1Hour": null,
    "4Hour": null,
  };

  console.log("[Scheduler] Scheduler stopped");
}

export function getSchedulerStatus(): SchedulerState & { schedules: Record<string, string>; marketOpen: boolean } {
  return {
    ...state,
    marketOpen: isMarketHours(),
    schedules: {
      "5Min": "*/5 * * * * (every 5 minutes)",
      "15Min": "*/15 * * * * (every 15 minutes)",
      "1Hour": "0 * * * * (every hour)",
      "4Hour": "0 */4 * * * (every 4 hours)",
    },
  };
}

export async function triggerManualScan(timeframe?: string): Promise<{ 
  timeframe: string; 
  matches: number;
  newEntries: number;
}[]> {
  const results: { timeframe: string; matches: number; newEntries: number }[] = [];
  
  if (timeframe) {
    await runTimeframeScan(timeframe);
    results.push({ 
      timeframe, 
      matches: state.results[timeframe]?.matches || 0,
      newEntries: state.results[timeframe]?.newEntries || 0,
    });
  } else {
    for (const tf of ["5Min", "15Min", "1Hour", "4Hour"]) {
      await runTimeframeScan(tf);
      results.push({ 
        timeframe: tf, 
        matches: state.results[tf]?.matches || 0,
        newEntries: state.results[tf]?.newEntries || 0,
      });
    }
  }
  
  return results;
}

export function getLatestNewEntries(): SignalStateChange[] {
  return state.latestNewEntries;
}

export function getLatestAuxiliaryMatches(): AuxiliaryMatchInfo[] {
  return state.latestAuxiliaryMatches;
}

export function isMarketHours(): boolean {
  const now = new Date();
  const nyTime = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
  const hours = nyTime.getHours();
  const minutes = nyTime.getMinutes();
  const day = nyTime.getDay();
  
  if (day === 0 || day === 6) return false;
  
  const timeInMinutes = hours * 60 + minutes;
  const extendedOpen = 4 * 60;   // 4:00 AM ET (pre-market start)
  const extendedClose = 20 * 60; // 8:00 PM ET (after-hours end)
  
  return timeInMinutes >= extendedOpen && timeInMinutes <= extendedClose;
}
