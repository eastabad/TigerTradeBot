import { useQuery, useMutation } from "@tanstack/react-query";
import { useParams, Redirect } from "wouter";
import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  DollarSign,
  TrendingUp,
  TrendingDown,
  Briefcase,
  RefreshCw,
  ListOrdered,
  Clock,
  CheckCircle,
  XCircle,
  AlertCircle,
  X,
  History,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import type { AlpacaAccount, AlpacaPosition, AlpacaOrder, Trade } from "@shared/schema";

interface ClosedTradesByAccount {
  [accountId: number]: Trade[];
}

interface AccountInfo {
  accountId: number;
  name: string;
  symbols: string[];
  account: AlpacaAccount;
}

interface PositionsByAccount {
  accountId: number;
  accountName: string;
  positions: AlpacaPosition[];
}

interface OrdersByAccount {
  accountId: number;
  accountName: string;
  orders: AlpacaOrder[];
}

interface ClosedTrade {
  symbol: string;
  direction: "long" | "short";
  entryPrice: number;
  exitPrice: number;
  quantity: number;
  pnl: number;
  pnlPercent: number;
  holdingTime: string;
  entryDate: Date;
  exitDate: Date;
}

function formatCurrency(value: string | number | null | undefined): string {
  if (value === null || value === undefined) return "-";
  const num = typeof value === "string" ? parseFloat(value) : value;
  if (isNaN(num)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(num);
}

function formatPercent(value: string | number): string {
  const num = typeof value === "string" ? parseFloat(value) : value;
  return `${num >= 0 ? "+" : ""}${(num * 100).toFixed(2)}%`;
}

function formatDateTime(dateString: string | null | undefined): string {
  if (!dateString) return "-";
  return new Date(dateString).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatDateTimePrice(date: Date, price: number): string {
  const dateStr = date.toLocaleString("en-US", {
    month: "numeric",
    day: "numeric",
    year: "2-digit",
    hour: "numeric",
    minute: "2-digit",
  });
  return `${dateStr} @ $${price.toFixed(2)}`;
}

function formatHoldingTime(entryDate: Date, exitDate: Date): string {
  const diffMs = exitDate.getTime() - entryDate.getTime();
  const diffMins = Math.floor(Math.abs(diffMs) / (1000 * 60));
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);
  
  const days = diffDays;
  const hours = diffHours % 24;
  const mins = diffMins % 60;
  
  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (mins > 0 || parts.length === 0) parts.push(`${mins}m`);
  
  return parts.join(" ");
}

interface PositionLot {
  qty: number;
  avgPrice: number;
  entryDate: Date;
}

interface PositionState {
  direction: "long" | "short" | null;
  lots: PositionLot[];
}

function computeClosedTrades(orders: AlpacaOrder[]): ClosedTrade[] {
  const filledOrders = orders
    .filter(o => o.status === "filled" && o.filledAvgPrice && o.filledAt)
    .sort((a, b) => new Date(a.filledAt!).getTime() - new Date(b.filledAt!).getTime());

  const closedTrades: ClosedTrade[] = [];
  const positionsBySymbol: Map<string, PositionState> = new Map();

  for (const order of filledOrders) {
    const symbol = order.symbol;
    const orderQty = parseFloat(order.filledQty || order.qty);
    const price = parseFloat(order.filledAvgPrice!);
    const date = new Date(order.filledAt!);
    const isBuy = order.side === "buy";
    
    if (!positionsBySymbol.has(symbol)) {
      positionsBySymbol.set(symbol, { direction: null, lots: [] });
    }
    const position = positionsBySymbol.get(symbol)!;

    if (position.direction === null || position.lots.length === 0) {
      position.direction = isBuy ? "long" : "short";
      position.lots.push({ qty: orderQty, avgPrice: price, entryDate: date });
    } else if (
      (position.direction === "long" && isBuy) ||
      (position.direction === "short" && !isBuy)
    ) {
      position.lots.push({ qty: orderQty, avgPrice: price, entryDate: date });
    } else {
      let remainingQty = orderQty;
      
      let totalMatchedQty = 0;
      let totalEntryNotional = 0;
      let totalPnl = 0;
      let earliestEntryDate: Date | null = null;
      const tradeDirection = position.direction;
      
      while (remainingQty > 0 && position.lots.length > 0) {
        const lot = position.lots[0];
        const matchQty = Math.min(remainingQty, lot.qty);
        
        totalMatchedQty += matchQty;
        totalEntryNotional += lot.avgPrice * matchQty;
        
        if (earliestEntryDate === null || lot.entryDate < earliestEntryDate) {
          earliestEntryDate = lot.entryDate;
        }
        
        if (tradeDirection === "long") {
          totalPnl += (price - lot.avgPrice) * matchQty;
        } else {
          totalPnl += (lot.avgPrice - price) * matchQty;
        }
        
        remainingQty -= matchQty;
        lot.qty -= matchQty;
        
        if (lot.qty <= 0) {
          position.lots.shift();
        }
      }
      
      if (totalMatchedQty > 0 && earliestEntryDate) {
        const weightedAvgEntryPrice = totalEntryNotional / totalMatchedQty;
        const pnlPercent = tradeDirection === "long"
          ? (price - weightedAvgEntryPrice) / weightedAvgEntryPrice
          : (weightedAvgEntryPrice - price) / weightedAvgEntryPrice;
        
        closedTrades.push({
          symbol,
          direction: tradeDirection,
          entryPrice: weightedAvgEntryPrice,
          exitPrice: price,
          quantity: totalMatchedQty,
          pnl: totalPnl,
          pnlPercent,
          holdingTime: formatHoldingTime(earliestEntryDate, date),
          entryDate: earliestEntryDate,
          exitDate: date,
        });
      }
      
      if (remainingQty > 0) {
        position.direction = isBuy ? "long" : "short";
        position.lots.push({ qty: remainingQty, avgPrice: price, entryDate: date });
      } else if (position.lots.length === 0) {
        position.direction = null;
      }
    }
  }

  return closedTrades.sort((a, b) => b.exitDate.getTime() - a.exitDate.getTime());
}

function StatusBadge({ status }: { status: string }) {
  const icons: Record<string, React.ReactNode> = {
    filled: <CheckCircle className="h-3 w-3" />,
    new: <Clock className="h-3 w-3" />,
    pending: <Clock className="h-3 w-3" />,
    held: <Clock className="h-3 w-3" />,
    accepted: <Clock className="h-3 w-3" />,
    partially_filled: <AlertCircle className="h-3 w-3" />,
    cancelled: <XCircle className="h-3 w-3" />,
    canceled: <XCircle className="h-3 w-3" />,
    rejected: <XCircle className="h-3 w-3" />,
    expired: <XCircle className="h-3 w-3" />,
  };

  const colors: Record<string, string> = {
    filled: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400",
    new: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400",
    pending: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400",
    held: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400",
    accepted: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400",
    partially_filled: "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-400",
    cancelled: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400",
    canceled: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400",
    rejected: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400",
    expired: "bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400",
  };

  return (
    <Badge variant="secondary" className={`gap-1 ${colors[status] || ""}`}>
      {icons[status]}
      {status.replace("_", " ")}
    </Badge>
  );
}

function SideBadge({ side }: { side: string }) {
  const isBuy = side.toLowerCase() === "buy";
  return (
    <Badge 
      variant="outline" 
      className={isBuy 
        ? "border-green-500 text-green-700 dark:text-green-400" 
        : "border-red-500 text-red-700 dark:text-red-400"
      }
    >
      {side.toUpperCase()}
    </Badge>
  );
}

export default function AccountDetail() {
  const params = useParams<{ accountId: string }>();
  const accountId = parseInt(params.accountId || "1", 10);
  const { toast } = useToast();

  const { data: accounts, isLoading: accountsLoading } = useQuery<AccountInfo[]>({
    queryKey: ["/api/accounts"],
  });

  const { data: positionsByAccount, isLoading: positionsLoading } = useQuery<PositionsByAccount[]>({
    queryKey: ["/api/positions/by-account"],
  });

  const { data: openOrdersByAccount, isLoading: openOrdersLoading } = useQuery<OrdersByAccount[]>({
    queryKey: ["/api/orders/by-account"],
  });

  const { data: orderHistoryByAccount, isLoading: orderHistoryLoading } = useQuery<OrdersByAccount[]>({
    queryKey: ["/api/orders/history/by-account"],
  });

  const { data: closedTradesByAccount } = useQuery<ClosedTradesByAccount>({
    queryKey: ["/api/trades/closed/by-account"],
  });

  const cancelOrderMutation = useMutation({
    mutationFn: async (orderId: string) => {
      return apiRequest("DELETE", `/api/orders/${orderId}`);
    },
    onSuccess: () => {
      toast({
        title: "Order Cancelled",
        description: "The order has been cancelled successfully.",
      });
      queryClient.invalidateQueries({ queryKey: ["/api/orders/by-account"] });
      queryClient.invalidateQueries({ queryKey: ["/api/orders/history/by-account"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Cancel Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const account = accounts?.find(a => a.accountId === accountId);
  const accountPositions = positionsByAccount?.find(p => p.accountId === accountId)?.positions || [];
  const accountOpenOrders = openOrdersByAccount?.find(o => o.accountId === accountId)?.orders || [];
  const accountOrderHistory = orderHistoryByAccount?.find(o => o.accountId === accountId)?.orders || [];

  const closedTrades = useMemo((): ClosedTrade[] => {
    // Use database closed trades which have accurate entry/exit prices from Alpaca
    const dbClosedTrades = closedTradesByAccount?.[accountId] || [];
    
    if (dbClosedTrades.length > 0) {
      return dbClosedTrades
        .filter((t): t is Trade => {
          if (!t) return false;
          // Only include trades that are actually filled with valid prices
          const hasValidExitPrice = (t.filledPrice ?? 0) > 0;
          const hasValidEntryPrice = (t.positionAvgEntryPrice ?? 0) > 0;
          return hasValidExitPrice && hasValidEntryPrice;
        })
        .map((trade): ClosedTrade => {
          const entryPrice = trade.positionAvgEntryPrice!;
          const exitPrice = trade.filledPrice!;
          const quantity = trade.positionQty ?? trade.quantity ?? 0;
          const direction = trade.positionSide === "short" ? "short" : "long";
          
          // Calculate P&L based on direction
          const pnl = direction === "long" 
            ? (exitPrice - entryPrice) * quantity
            : (entryPrice - exitPrice) * quantity;
          const pnlPercent = entryPrice > 0 ? pnl / (entryPrice * quantity) : 0;
          
          // Calculate holding time - use updatedAt as fallback for exit time
          const entryDate = trade.positionEntryDate ? new Date(trade.positionEntryDate) : new Date(trade.createdAt ?? new Date());
          const exitDate = trade.filledAt ? new Date(trade.filledAt) : (trade.updatedAt ? new Date(trade.updatedAt) : new Date(trade.createdAt ?? new Date()));
          const holdingMs = exitDate.getTime() - entryDate.getTime();
          const holdingDays = Math.floor(holdingMs / (1000 * 60 * 60 * 24));
          const holdingHours = Math.floor((holdingMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
          const holdingTime = holdingDays > 0 
            ? `${holdingDays}d ${holdingHours}h` 
            : holdingHours > 0 
              ? `${holdingHours}h` 
              : "< 1h";
          
          return {
            symbol: trade.symbol,
            direction,
            entryPrice,
            exitPrice,
            quantity,
            pnl,
            pnlPercent,
            holdingTime,
            entryDate,
            exitDate,
          };
        })
        .sort((a, b) => b.exitDate.getTime() - a.exitDate.getTime());
    }
    
    // Fallback to computing from order history if no database trades
    return computeClosedTrades(accountOrderHistory);
  }, [closedTradesByAccount, accountId, accountOrderHistory]);

  if (!accountsLoading && !account) {
    return <Redirect to="/" />;
  }

  const portfolioValue = account?.account.portfolioValue ? parseFloat(account.account.portfolioValue) : 0;
  const lastEquity = account?.account.lastEquity ? parseFloat(account.account.lastEquity) : 0;
  const dayChange = portfolioValue - lastEquity;
  const dayChangePercent = lastEquity > 0 ? (dayChange / lastEquity) : 0;

  const initialCapital = 100000;
  const totalReturn = portfolioValue - initialCapital;
  const totalReturnPercent = totalReturn / initialCapital;

  const totalMarketValue = accountPositions.reduce((sum, pos) => sum + parseFloat(pos.marketValue), 0);
  const totalUnrealizedPl = accountPositions.reduce((sum, pos) => sum + parseFloat(pos.unrealizedPl), 0);
  const totalCostBasis = accountPositions.reduce((sum, pos) => sum + parseFloat(pos.costBasis), 0);
  const totalPlPercent = totalCostBasis > 0 ? (totalUnrealizedPl / totalCostBasis) : 0;

  const handleRefresh = () => {
    queryClient.invalidateQueries({ queryKey: ["/api/accounts"] });
    queryClient.invalidateQueries({ queryKey: ["/api/positions/by-account"] });
    queryClient.invalidateQueries({ queryKey: ["/api/orders/by-account"] });
    queryClient.invalidateQueries({ queryKey: ["/api/orders/history/by-account"] });
    queryClient.invalidateQueries({ queryKey: ["/api/trades/closed/by-account"] });
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold" data-testid="text-page-title">
            {accountsLoading ? <Skeleton className="h-8 w-64" /> : account?.name}
          </h1>
          <p className="text-muted-foreground">
            {accountsLoading ? (
              <Skeleton className="h-4 w-48 mt-1" />
            ) : (
              <>
                Account: {account?.account.accountNumber} | 
                Symbols: {account?.symbols.length ? account.symbols.join(", ") : "All other"}
              </>
            )}
          </p>
        </div>
        <Button
          variant="outline"
          onClick={handleRefresh}
          disabled={accountsLoading || positionsLoading}
          data-testid="button-refresh-account"
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${accountsLoading || positionsLoading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card data-testid="card-portfolio-value">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Portfolio Value</CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {accountsLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <>
                <div className="text-2xl font-bold font-mono" data-testid="text-portfolio-value">
                  {formatCurrency(portfolioValue)}
                </div>
                <p className={`text-xs flex items-center gap-1 ${totalReturn >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                  {totalReturn >= 0 ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                  {formatCurrency(totalReturn)} ({formatPercent(totalReturnPercent)}) total
                </p>
              </>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-market-value">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Market Value</CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {positionsLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <>
                <div className="text-2xl font-bold font-mono" data-testid="text-market-value">
                  {formatCurrency(totalMarketValue)}
                </div>
                <p className="text-xs text-muted-foreground">
                  From {accountPositions.length} positions
                </p>
              </>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-unrealized-pl">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Unrealized P&L</CardTitle>
            {totalUnrealizedPl >= 0 ? (
              <TrendingUp className="h-4 w-4 text-green-600 dark:text-green-400" />
            ) : (
              <TrendingDown className="h-4 w-4 text-red-600 dark:text-red-400" />
            )}
          </CardHeader>
          <CardContent>
            {positionsLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <>
                <div className={`text-2xl font-bold font-mono ${totalUnrealizedPl >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`} data-testid="text-unrealized-pl">
                  {formatCurrency(totalUnrealizedPl)}
                </div>
                <p className={`text-xs ${totalUnrealizedPl >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                  {formatPercent(totalPlPercent)}
                </p>
              </>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-day-change">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Day Change</CardTitle>
            {dayChange >= 0 ? (
              <TrendingUp className="h-4 w-4 text-green-600 dark:text-green-400" />
            ) : (
              <TrendingDown className="h-4 w-4 text-red-600 dark:text-red-400" />
            )}
          </CardHeader>
          <CardContent>
            {accountsLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <>
                <div className={`text-2xl font-bold font-mono ${dayChange >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`} data-testid="text-day-change">
                  {formatCurrency(dayChange)}
                </div>
                <p className={`text-xs ${dayChange >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                  {formatPercent(dayChangePercent)} today
                </p>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <Card data-testid="card-account-info">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Account Details</CardTitle>
        </CardHeader>
        <CardContent>
          {accountsLoading ? (
            <Skeleton className="h-20 w-full" />
          ) : (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div>
                <p className="text-sm text-muted-foreground">Buying Power</p>
                <p className="font-mono font-semibold">{formatCurrency(account?.account.buyingPower || 0)}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Cash</p>
                <p className="font-mono font-semibold">{formatCurrency(account?.account.cash || 0)}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Status</p>
                <Badge variant={account?.account.status === "ACTIVE" ? "default" : "secondary"}>
                  {account?.account.status}
                </Badge>
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Day Trades</p>
                <p className="font-mono font-semibold">{account?.account.daytradeCount || 0}</p>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      <Tabs defaultValue="positions" className="space-y-4">
        <TabsList data-testid="tabs-account">
          <TabsTrigger value="positions" data-testid="tab-positions">
            Positions
            {accountPositions.length > 0 && (
              <Badge variant="secondary" className="ml-2">
                {accountPositions.length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="open-orders" data-testid="tab-open-orders">
            Open Orders
            {accountOpenOrders.length > 0 && (
              <Badge variant="secondary" className="ml-2">
                {accountOpenOrders.length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="closed-orders" data-testid="tab-closed-orders">
            Closed Orders
            {closedTrades.length > 0 && (
              <Badge variant="secondary" className="ml-2">
                {closedTrades.length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="order-history" data-testid="tab-order-history">
            Order History
          </TabsTrigger>
        </TabsList>

        <TabsContent value="positions">
          <Card data-testid="card-positions">
            <CardHeader className="flex flex-row items-center justify-between gap-2">
              <CardTitle className="flex items-center gap-2">
                <Briefcase className="h-5 w-5" />
                Positions
              </CardTitle>
            </CardHeader>
            <CardContent>
              {positionsLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-12 w-full" />
                  ))}
                </div>
              ) : accountPositions.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead className="text-right">Avg Cost</TableHead>
                      <TableHead className="text-right">Current</TableHead>
                      <TableHead className="text-right">Market Value</TableHead>
                      <TableHead className="text-right">P&L</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {accountPositions.map((pos) => {
                      const pnl = parseFloat(pos.unrealizedPl);
                      const pnlPercent = parseFloat(pos.unrealizedPlpc);
                      return (
                        <TableRow key={pos.symbol} data-testid={`row-position-${pos.symbol}`}>
                          <TableCell className="font-medium font-mono">{pos.symbol}</TableCell>
                          <TableCell className="text-right font-mono">{pos.qty}</TableCell>
                          <TableCell className="text-right font-mono">{formatCurrency(pos.avgEntryPrice)}</TableCell>
                          <TableCell className="text-right font-mono">{formatCurrency(pos.currentPrice)}</TableCell>
                          <TableCell className="text-right font-mono">{formatCurrency(pos.marketValue)}</TableCell>
                          <TableCell className={`text-right font-mono ${pnl >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                            {formatCurrency(pnl)}
                            <span className="text-xs ml-1">({formatPercent(pnlPercent)})</span>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              ) : (
                <div className="flex flex-col items-center justify-center py-8 text-center text-muted-foreground">
                  <Briefcase className="h-12 w-12 mb-3 opacity-50" />
                  <p className="font-medium">No positions</p>
                  <p className="text-sm">Open positions for this account will appear here</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="open-orders">
          <Card data-testid="card-open-orders">
            <CardHeader className="flex flex-row items-center justify-between gap-2">
              <CardTitle className="flex items-center gap-2">
                <ListOrdered className="h-5 w-5" />
                Open Orders
              </CardTitle>
            </CardHeader>
            <CardContent>
              {openOrdersLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-12 w-full" />
                  ))}
                </div>
              ) : accountOpenOrders.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Side</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead className="text-right">Price</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead className="text-right">Filled</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Submitted</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {accountOpenOrders.map((order) => (
                      <TableRow key={order.id} data-testid={`row-order-${order.id}`}>
                        <TableCell className="font-medium font-mono">{order.symbol}</TableCell>
                        <TableCell><SideBadge side={order.side} /></TableCell>
                        <TableCell>
                          <Badge variant="outline" className="font-mono text-xs">
                            {order.type.toUpperCase()}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right font-mono">
                          {order.limitPrice ? `$${parseFloat(order.limitPrice).toFixed(2)}` : 
                           order.stopPrice ? `$${parseFloat(order.stopPrice).toFixed(2)}` : 
                           "Market"}
                        </TableCell>
                        <TableCell className="text-right font-mono">{order.qty}</TableCell>
                        <TableCell className="text-right font-mono">{order.filledQty || "0"}</TableCell>
                        <TableCell><StatusBadge status={order.status} /></TableCell>
                        <TableCell className="text-right text-xs font-mono text-muted-foreground">
                          {formatDateTime(order.submittedAt)}
                        </TableCell>
                        <TableCell className="text-right">
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon"
                                disabled={cancelOrderMutation.isPending}
                                data-testid={`button-cancel-order-${order.id}`}
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                              <AlertDialogHeader>
                                <AlertDialogTitle>Cancel Order</AlertDialogTitle>
                                <AlertDialogDescription>
                                  Are you sure you want to cancel this {order.side.toUpperCase()} order for {order.qty} shares of {order.symbol}?
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel>Keep Order</AlertDialogCancel>
                                <AlertDialogAction
                                  onClick={() => cancelOrderMutation.mutate(order.id)}
                                >
                                  Cancel Order
                                </AlertDialogAction>
                              </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="flex flex-col items-center justify-center py-8 text-center text-muted-foreground">
                  <ListOrdered className="h-12 w-12 mb-3 opacity-50" />
                  <p className="font-medium">No open orders</p>
                  <p className="text-sm">Active orders for this account will appear here</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="closed-orders">
          <Card data-testid="card-closed-orders">
            <CardHeader className="flex flex-row items-center justify-between gap-2">
              <CardTitle className="flex items-center gap-2">
                <History className="h-5 w-5" />
                Closed Orders
              </CardTitle>
            </CardHeader>
            <CardContent>
              {orderHistoryLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3, 4, 5].map((i) => (
                    <Skeleton key={i} className="h-12 w-full" />
                  ))}
                </div>
              ) : closedTrades.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Direction</TableHead>
                      <TableHead className="text-right">Entry</TableHead>
                      <TableHead className="text-right">Exit</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead className="text-right">P&L ($)</TableHead>
                      <TableHead className="text-right">P&L (%)</TableHead>
                      <TableHead className="text-right">Holding Time</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {closedTrades.map((trade, index) => (
                      <TableRow key={`${trade.symbol}-${index}`} data-testid={`row-closed-${trade.symbol}-${index}`}>
                        <TableCell className="font-medium font-mono">{trade.symbol}</TableCell>
                        <TableCell>
                          <Badge 
                            variant="outline" 
                            className={trade.direction === "long" 
                              ? "border-green-500 text-green-700 dark:text-green-400" 
                              : "border-red-500 text-red-700 dark:text-red-400"
                            }
                          >
                            {trade.direction.toUpperCase()}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs">{formatDateTimePrice(trade.entryDate, trade.entryPrice)}</TableCell>
                        <TableCell className="text-right font-mono text-xs">{formatDateTimePrice(trade.exitDate, trade.exitPrice)}</TableCell>
                        <TableCell className="text-right font-mono">{trade.quantity}</TableCell>
                        <TableCell className={`text-right font-mono ${trade.pnl >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                          {formatCurrency(trade.pnl)}
                        </TableCell>
                        <TableCell className={`text-right font-mono ${trade.pnlPercent >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                          {formatPercent(trade.pnlPercent)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          {trade.holdingTime}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="flex flex-col items-center justify-center py-8 text-center text-muted-foreground">
                  <History className="h-12 w-12 mb-3 opacity-50" />
                  <p className="font-medium">No closed orders</p>
                  <p className="text-sm">Completed round-trip trades will appear here</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="order-history">
          <Card data-testid="card-order-history">
            <CardHeader className="flex flex-row items-center justify-between gap-2">
              <CardTitle className="flex items-center gap-2">
                <Clock className="h-5 w-5" />
                Order History
              </CardTitle>
            </CardHeader>
            <CardContent>
              {orderHistoryLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3, 4, 5].map((i) => (
                    <Skeleton key={i} className="h-12 w-full" />
                  ))}
                </div>
              ) : accountOrderHistory.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Side</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead className="text-right">Order Price</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead className="text-right">Filled</TableHead>
                      <TableHead className="text-right">Filled Price</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Submitted</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {accountOrderHistory.map((order) => (
                      <TableRow key={order.id} data-testid={`row-order-history-${order.id}`}>
                        <TableCell className="font-medium font-mono">{order.symbol}</TableCell>
                        <TableCell><SideBadge side={order.side} /></TableCell>
                        <TableCell>
                          <Badge variant="outline" className="font-mono text-xs">
                            {order.type.toUpperCase()}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right font-mono">
                          {order.limitPrice ? `$${parseFloat(order.limitPrice).toFixed(2)}` : 
                           order.stopPrice ? `$${parseFloat(order.stopPrice).toFixed(2)}` : 
                           "Market"}
                        </TableCell>
                        <TableCell className="text-right font-mono">{order.qty}</TableCell>
                        <TableCell className="text-right font-mono">{order.filledQty || "0"}</TableCell>
                        <TableCell className="text-right font-mono">
                          {order.filledAvgPrice ? formatCurrency(order.filledAvgPrice) : "-"}
                        </TableCell>
                        <TableCell><StatusBadge status={order.status} /></TableCell>
                        <TableCell className="text-right text-xs font-mono text-muted-foreground">
                          {formatDateTime(order.submittedAt)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="flex flex-col items-center justify-center py-8 text-center text-muted-foreground">
                  <Clock className="h-12 w-12 mb-3 opacity-50" />
                  <p className="font-medium">No order history</p>
                  <p className="text-sm">Completed orders for this account will appear here</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
