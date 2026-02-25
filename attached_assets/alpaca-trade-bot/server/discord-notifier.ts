import { storage } from "./storage";

interface TradeNotificationData {
  symbol: string;
  side: string;
  quantity: number;
  filledPrice: number;
  totalAmount: number;
  status: string;
  orderId: string;
  accountId?: number;
  sentiment?: string;
  stopLoss?: number;
  takeProfit?: number;
  timeframe?: string;
  indicator?: string;
  lastSupplyText?: string;
  lastDemandText?: string;
  oscrating?: number;
  trendrating?: number;
  risk?: number;
}

// Get webhook URLs for an account (returns array of 1-2 webhooks)
export function getWebhookUrlsForAccount(accountId: number): string[] {
  const urls: string[] = [];
  
  switch (accountId) {
    case 1:
      if (process.env.DISCORD_WEBHOOK_ACCOUNT1_1) urls.push(process.env.DISCORD_WEBHOOK_ACCOUNT1_1);
      if (process.env.DISCORD_WEBHOOK_ACCOUNT1_2) urls.push(process.env.DISCORD_WEBHOOK_ACCOUNT1_2);
      break;
    case 2:
      if (process.env.DISCORD_WEBHOOK_ACCOUNT2_1) urls.push(process.env.DISCORD_WEBHOOK_ACCOUNT2_1);
      if (process.env.DISCORD_WEBHOOK_ACCOUNT2_2) urls.push(process.env.DISCORD_WEBHOOK_ACCOUNT2_2);
      break;
    case 3:
      if (process.env.DISCORD_WEBHOOK_ACCOUNT3_1) urls.push(process.env.DISCORD_WEBHOOK_ACCOUNT3_1);
      if (process.env.DISCORD_WEBHOOK_ACCOUNT3_2) urls.push(process.env.DISCORD_WEBHOOK_ACCOUNT3_2);
      break;
  }
  
  // Fallback to global webhook if no account-specific webhooks configured
  if (urls.length === 0 && process.env.DISCORD_WEBHOOK_URL) {
    urls.push(process.env.DISCORD_WEBHOOK_URL);
  }
  
  return urls;
}

function riskToStars(risk?: number): string {
  if (!risk) return "⭐";
  const stars = Math.min(Math.max(Math.round(risk), 1), 5);
  return "⭐".repeat(stars);
}

function cleanString(str: string): string {
  return str.replace(/<[^>]*>/g, "").trim();
}

export async function sendDiscordNotification(
  webhookUrl: string,
  data: TradeNotificationData
): Promise<boolean> {
  if (!webhookUrl) {
    console.log("Discord webhook URL not configured, skipping notification");
    return false;
  }

  try {
    const logoApiToken = process.env.LOGO_DEV_API_TOKEN;
    const thumbnailUrl = data.symbol && logoApiToken
      ? `https://img.logo.dev/ticker/${data.symbol}?token=${logoApiToken}&format=png&retina=true`
      : undefined;

    const currentDate = new Date();
    const timestamp = currentDate.toISOString();

    const isExit = data.sentiment === "flat";
    const signalType = isExit
      ? data.side === "buy"
        ? "ExitShort"
        : "ExitLong"
      : data.side === "buy"
      ? "Long"
      : "Short";

    let color: number;
    let emoji: string;
    let statusEmoji: string;

    if (data.status === "filled") {
      statusEmoji = "✅";
      if (isExit) {
        color = 16776960; // Yellow
        emoji = "🟨";
      } else if (data.side === "buy") {
        color = 65280; // Green
        emoji = "🟩";
      } else {
        color = 16711680; // Red
        emoji = "🟥";
      }
    } else if (data.status === "rejected" || data.status === "canceled") {
      statusEmoji = "❌";
      color = 16711680; // Red
      emoji = "🟥";
    } else {
      statusEmoji = "⏳";
      color = 8421504; // Gray
      emoji = "⬜";
    }

    const estOptions: Intl.DateTimeFormatOptions = {
      timeZone: "America/New_York",
    };
    const estDate = currentDate.toLocaleDateString("en-US", estOptions);
    const estTime = currentDate.toLocaleTimeString("en-US", estOptions);

    const fields: Array<{ name: string; value: string; inline: boolean }> = [];

    if (data.status === "filled") {
      fields.push({
        name: "💰 Filled Price",
        value: `$${data.filledPrice.toFixed(2)}`,
        inline: true,
      });
      fields.push({
        name: "📊 Total Amount",
        value: `$${data.totalAmount.toFixed(2)}`,
        inline: true,
      });
      fields.push({
        name: "📦 Quantity",
        value: String(data.quantity),
        inline: true,
      });
    }

    if (!isExit) {
      if (data.takeProfit) {
        fields.push({
          name: "🎯 Take Profit",
          value: `$${data.takeProfit.toFixed(2)}`,
          inline: true,
        });
      }
      if (data.stopLoss) {
        fields.push({
          name: "⛔️ Stop Loss",
          value: `$${data.stopLoss.toFixed(2)}`,
          inline: true,
        });
      }
      if (data.timeframe) {
        fields.push({
          name: "🕖 Timeframe",
          value: cleanString(data.timeframe),
          inline: true,
        });
      }

      if (data.indicator) {
        const indicatorValue = String(data.indicator)
          .replace(/<br>/g, "\n")
          .substring(0, 1024);
        fields.push({
          name: "⚛️ AI Decision",
          value: indicatorValue,
          inline: false,
        });
      }

    }

    if (data.timeframe && isExit) {
      fields.push({
        name: "Timeframe",
        value: data.timeframe,
        inline: true,
      });
    }
    fields.push({ name: "Date", value: estDate, inline: true });
    fields.push({ name: "Time", value: estTime, inline: true });

    const rating =
      ((data.oscrating || 0) + (data.trendrating || 0)).toFixed(0);
    const riskStars = riskToStars(data.risk);

    const descriptionLines = [
      `**Status**: ${statusEmoji} ${data.status.toUpperCase()}`,
      `**Action**: ${signalType}`,
      `**Ticker**: ${data.symbol}`,
    ];

    if (data.status === "filled") {
      descriptionLines.push(`**Price**: $${data.filledPrice.toFixed(2)}`);
    }


    const description = descriptionLines.join("\n");

    const embed = {
      embeds: [
        {
          title: `${statusEmoji} ${signalType} - ${data.symbol}`,
          description: description,
          color: color,
          ...(thumbnailUrl && { thumbnail: { url: thumbnailUrl } }),
          fields: fields,
          timestamp: timestamp,
          footer: {
            text: `Disclaimer: Auto-generated notification.\nNot investment advice.`,
          },
        },
      ],
    };

    const response = await fetch(webhookUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(embed),
    });

    if (!response.ok) {
      console.error(
        `Discord notification failed: ${response.status} ${response.statusText}`
      );
      return false;
    }

    console.log(`Discord notification sent for ${data.symbol} (${data.status})`);
    return true;
  } catch (error) {
    console.error("Error sending Discord notification:", error);
    return false;
  }
}

// Send to multiple webhooks for an account
async function sendToMultipleWebhooks(
  accountId: number,
  data: TradeNotificationData
): Promise<void> {
  const webhookUrls = getWebhookUrlsForAccount(accountId);
  
  if (webhookUrls.length === 0) {
    console.log(`No Discord webhooks configured for Account ${accountId}`);
    return;
  }

  // Send to all configured webhooks in parallel
  await Promise.all(
    webhookUrls.map((url) => sendDiscordNotification(url, data))
  );
}

export async function pollOrderAndNotify(
  alpacaClient: any,
  orderId: string,
  accountId: number,
  signalData: any
): Promise<void> {
  const webhookUrls = getWebhookUrlsForAccount(accountId);
  const hasWebhooks = webhookUrls.length > 0;
  
  if (!hasWebhooks) {
    console.log(`No Discord webhooks configured for Account ${accountId}, but will still update trade records`);
  }

  let attempts = 0;
  let currentInterval = 3000; // Start with 3 seconds
  const maxInterval = 60000;  // Max 1 minute between checks
  const backoffMultiplier = 1.5; // Increase interval by 50% each time

  const poll = async () => {
    attempts++;
    try {
      const order = await alpacaClient.getOrderById(orderId);

      if (!order) {
        console.log(`Order ${orderId} not found, stopping poll`);
        return;
      }

      const status = order.status.toLowerCase();

      // Only stop on fully filled status, continue polling for partially_filled
      if (status === "filled") {
        const filledPrice = parseFloat(order.filledAvgPrice || "0");
        const filledQty = parseFloat(order.filledQty || order.qty);

        // Update the trade record with filled price, quantity, and time
        try {
          await storage.updateTradeByOrderId(orderId, {
            status: status,
            filledPrice: filledPrice,
            filledQuantity: filledQty,
            filledAt: order.filledAt ? new Date(order.filledAt) : new Date(),
          });
          console.log(`Trade record updated with filled price: $${filledPrice.toFixed(2)} at ${order.filledAt}`);
        } catch (dbError) {
          console.error("Error updating trade with filled price:", dbError);
        }

        // Only send Discord notification if webhooks are configured
        if (hasWebhooks) {
          await sendToMultipleWebhooks(accountId, {
            symbol: order.symbol,
            side: order.side,
            quantity: filledQty,
            filledPrice: filledPrice,
            totalAmount: filledPrice * filledQty,
            status: status,
            orderId: order.id,
            accountId: accountId,
            sentiment: signalData?.sentiment,
            stopLoss: signalData?.stopLoss?.stopPrice || signalData?.stopLoss,
            takeProfit: signalData?.takeProfit?.limitPrice || signalData?.takeProfit,
            timeframe: signalData?.extras?.timeframe,
            indicator: signalData?.extras?.indicator,
            lastSupplyText:
              signalData?.data?.lastSupplyText || signalData?.extras?.lastSupplyText,
            lastDemandText:
              signalData?.data?.lastDemandText || signalData?.extras?.lastDemandText,
            oscrating: signalData?.extras?.oscrating,
            trendrating: signalData?.extras?.trendrating,
            risk: signalData?.extras?.risk,
          });
        }
        return;
      }

      // Continue polling for partially_filled
      if (status === "partially_filled") {
        // Update partial fill info in database
        const filledPrice = parseFloat(order.filledAvgPrice || "0");
        const filledQty = parseFloat(order.filledQty || "0");
        try {
          await storage.updateTradeByOrderId(orderId, {
            status: status,
            filledPrice: filledPrice,
            filledQuantity: filledQty,
          });
        } catch (dbError) {
          console.error("Error updating partial fill:", dbError);
        }
        // Continue polling with backoff
        currentInterval = Math.min(currentInterval * backoffMultiplier, maxInterval);
        setTimeout(poll, currentInterval);
        return;
      }

      if (
        status === "canceled" ||
        status === "expired" ||
        status === "rejected"
      ) {
        // Update trade status in database
        try {
          await storage.updateTradeByOrderId(orderId, {
            status: status,
          });
        } catch (dbError) {
          console.error("Error updating cancelled trade:", dbError);
        }

        if (hasWebhooks) {
          await sendToMultipleWebhooks(accountId, {
            symbol: order.symbol,
            side: order.side,
            quantity: parseFloat(order.qty),
            filledPrice: 0,
            totalAmount: 0,
            status: status,
            orderId: order.id,
            accountId: accountId,
            sentiment: signalData?.sentiment,
            timeframe: signalData?.extras?.timeframe,
          });
        }
        return;
      }

      // For pending/new/accepted orders, continue polling with exponential backoff
      // Log progress every 20 attempts
      if (attempts % 20 === 0) {
        console.log(`Order ${orderId} still ${status} after ${attempts} attempts, next check in ${Math.round(currentInterval/1000)}s`);
      }
      
      // Increase interval with backoff, max 1 minute
      currentInterval = Math.min(currentInterval * backoffMultiplier, maxInterval);
      setTimeout(poll, currentInterval);
      
    } catch (error) {
      console.error(`Error polling order ${orderId}:`, error);
      // On error, continue polling with backoff
      currentInterval = Math.min(currentInterval * backoffMultiplier, maxInterval);
      setTimeout(poll, currentInterval);
    }
  };

  setTimeout(poll, currentInterval);
}
