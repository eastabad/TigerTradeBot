import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { CheckCircle, XCircle, Clock, Code, ChevronDown } from "lucide-react";
import { format } from "date-fns";

interface SignalLog {
  id: string;
  rawSignal: string;
  parsedSuccessfully: boolean;
  errorMessage: string | null;
  ipAddress: string | null;
  tradeId: string | null;
  createdAt: string;
}

interface ParsedSignal {
  action?: string;
  ticker?: string;
  symbol?: string;
  quantity?: number | string;
  sentiment?: string;
  extras?: {
    timeframe?: string;
    indicator?: string;
    referencePrice?: number;
    filter_status?: string;
  };
  data?: {
    symbol?: string;
  };
  stopLoss?: { stopPrice?: number } | number;
  takeProfit?: { limitPrice?: number } | number;
}

function parseSignalData(rawSignal: string): ParsedSignal | null {
  try {
    return JSON.parse(rawSignal);
  } catch {
    return null;
  }
}

function formatJson(rawSignal: string): string {
  try {
    return JSON.stringify(JSON.parse(rawSignal), null, 2);
  } catch {
    return rawSignal;
  }
}

function SignalLogCard({ log }: { log: SignalLog }) {
  const [isOpen, setIsOpen] = useState(false);
  const parsed = parseSignalData(log.rawSignal);
  const symbol = parsed?.ticker || parsed?.symbol || parsed?.data?.symbol || "Unknown";
  const action = parsed?.action || "unknown";
  const quantity = parsed?.quantity || "-";
  const timeframe = parsed?.extras?.timeframe || "-";
  const referencePrice = parsed?.extras?.referencePrice;
  const filterStatus = parsed?.extras?.filter_status;
  const isFlat = parsed?.sentiment === "flat";

  return (
    <div className="border-b border-border last:border-b-0">
      <div className="flex items-start gap-3 p-3">
        <div className="mt-0.5">
          {log.parsedSuccessfully ? (
            <CheckCircle className="h-5 w-5 text-green-500" />
          ) : (
            <XCircle className="h-5 w-5 text-red-500" />
          )}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium">{symbol}</span>
            <Badge variant={action === "buy" ? "default" : action === "sell" ? "secondary" : "outline"}>
              {isFlat ? "FLAT" : action.toUpperCase()}
            </Badge>
            {quantity !== "-" && (
              <Badge variant="outline">{quantity} shares</Badge>
            )}
            {referencePrice && (
              <Badge variant="outline">${referencePrice.toFixed(2)}</Badge>
            )}
            {timeframe !== "-" && (
              <Badge variant="outline">{timeframe}</Badge>
            )}
          </div>
          {filterStatus && (
            <p className="text-xs text-muted-foreground mt-1">{filterStatus}</p>
          )}
          {log.errorMessage && (
            <div className="mt-2 p-2 bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 rounded-md">
              <p className="text-sm text-red-600 dark:text-red-400 font-medium">
                Alpaca Error: {log.errorMessage}
              </p>
            </div>
          )}
          <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
            <Clock className="h-3 w-3" />
            <span>{format(new Date(log.createdAt), "MM/dd HH:mm:ss")}</span>
            {log.tradeId && (
              <span className="text-green-600">Trade executed</span>
            )}
          </div>
        </div>
        <Button 
          variant="ghost" 
          size="sm" 
          onClick={() => setIsOpen(!isOpen)}
          data-testid={`button-view-json-${log.id}`}
        >
          <Code className="h-4 w-4 mr-1" />
          JSON
          <ChevronDown className={`h-3 w-3 ml-1 transition-transform ${isOpen ? "rotate-180" : ""}`} />
        </Button>
      </div>
      {isOpen && (
        <div className="px-3 pb-3">
          <pre className="text-xs bg-muted p-3 rounded-md overflow-x-auto whitespace-pre-wrap break-all">
            {formatJson(log.rawSignal)}
          </pre>
        </div>
      )}
    </div>
  );
}

export default function SignalLogs() {
  const { data: logs, isLoading } = useQuery<SignalLog[]>({
    queryKey: ["/api/signal-logs"],
    refetchInterval: 30000,
  });

  return (
    <div className="p-4 md:p-6 space-y-4">
      <div className="flex items-center justify-between gap-4">
        <h1 className="text-2xl font-bold">Webhook Signals</h1>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <span>{logs?.length || 0} signals</span>
        </div>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-lg flex items-center justify-between gap-2">
            <span>Recent Signals</span>
            <div className="flex items-center gap-4 text-sm font-normal">
              <div className="flex items-center gap-1">
                <CheckCircle className="h-4 w-4 text-green-500" />
                <span>Success</span>
              </div>
              <div className="flex items-center gap-1">
                <XCircle className="h-4 w-4 text-red-500" />
                <span>Failed</span>
              </div>
            </div>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <ScrollArea className="h-[calc(100vh-220px)]">
            {isLoading ? (
              <div className="p-4 space-y-3">
                {[1, 2, 3, 4, 5].map((i) => (
                  <Skeleton key={i} className="h-16 w-full" />
                ))}
              </div>
            ) : logs && logs.length > 0 ? (
              logs.map((log) => (
                <SignalLogCard key={log.id} log={log} />
              ))
            ) : (
              <div className="p-8 text-center text-muted-foreground">
                No webhook signals received yet
              </div>
            )}
          </ScrollArea>
        </CardContent>
      </Card>
    </div>
  );
}
