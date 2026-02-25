/**
 * Discord Webhook Notifier
 * Sends scan results to Discord channels as table images
 */

import { SignalStateChange, AuxiliaryMatchInfo } from "./strategy-engine";
import { generateBothTables } from "./table-image-generator";

const WEBHOOK_URLS = [
  process.env.DISCORD_WEBHOOK_ACCOUNT3_1,
  process.env.DISCORD_WEBHOOK_ACCOUNT3_2,
].filter(Boolean) as string[];

const TF_LABELS: Record<string, string> = {
  "5Min": "5分钟",
  "15Min": "15分钟",
  "1Hour": "1小时",
  "4Hour": "4小时",
};

async function sendImageToWebhook(
  url: string, 
  imageBuffer: Buffer, 
  filename: string,
  content?: string
): Promise<boolean> {
  try {
    const blob = new Blob([imageBuffer], { type: "image/png" });
    const formData = new FormData();
    
    if (content) {
      formData.append("content", content);
    }
    
    formData.append("file", blob, filename);

    const response = await fetch(url, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const text = await response.text();
      console.error(`[Discord] Webhook failed: ${response.status} ${response.statusText} - ${text}`);
      return false;
    }
    return true;
  } catch (error) {
    console.error("[Discord] Webhook error:", error);
    return false;
  }
}

export async function sendScanResultsToDiscord(
  timeframe: string,
  currentMatches: SignalStateChange[],
  auxiliaryMatches: AuxiliaryMatchInfo[]
): Promise<void> {
  if (WEBHOOK_URLS.length === 0) {
    console.log("[Discord] No webhook URLs configured");
    return;
  }

  console.log(`[Discord] Processing ${currentMatches.length} current matches, aux: ${auxiliaryMatches.map(a => `${a.timeframe}:${a.matches.length}`).join(", ")}`);

  // Only send Discord notification when there are NEW entry signals in current scan timeframe
  // Auxiliary matches are just for reference, they should NOT trigger notification
  if (currentMatches.length === 0) {
    console.log(`[Discord] No NEW entries for ${timeframe} scan, skipping notification`);
    return;
  }

  const { long: longImage, short: shortImage } = generateBothTables(
    timeframe,
    currentMatches,
    auxiliaryMatches
  );

  console.log(`[Discord] Generated images - LONG: ${longImage ? longImage.length + ' bytes' : 'null'}, SHORT: ${shortImage ? shortImage.length + ' bytes' : 'null'}`);

  const scanLabel = TF_LABELS[timeframe] || timeframe;
  const timestamp = new Date().toLocaleString("en-US", { 
    timeZone: "America/New_York",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });

  let imagesSent = 0;

  for (const url of WEBHOOK_URLS) {
    if (shortImage) {
      const success = await sendImageToWebhook(
        url,
        shortImage,
        `${timeframe}_short_${Date.now()}.png`,
        `**📡 ${scanLabel}级别 TD AIScaner 扫描完成** - ${timestamp} ET`
      );
      if (success) {
        imagesSent++;
        console.log(`[Discord] Successfully sent SHORT table to webhook`);
      }
    }

    if (longImage) {
      const success = await sendImageToWebhook(
        url,
        longImage,
        `${timeframe}_long_${Date.now()}.png`
      );
      if (success) {
        imagesSent++;
        console.log(`[Discord] Successfully sent LONG table to webhook`);
      }
    }
  }

  console.log(`[Discord] Sent ${imagesSent} images to ${WEBHOOK_URLS.length} webhooks`);
}
