import { useQuery, useMutation } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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
  Briefcase,
  TrendingUp,
  TrendingDown,
  RefreshCw,
  X,
  DollarSign,
  Percent,
} from "lucide-react";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import type { AlpacaPosition } from "@shared/schema";

function formatCurrency(value: string | number): string {
  const num = typeof value === "string" ? parseFloat(value) : value;
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

function PnLDisplay({ value, percent }: { value: string; percent: string }) {
  const numValue = parseFloat(value);
  const numPercent = parseFloat(percent);
  const isPositive = numValue >= 0;

  return (
    <div className={`flex flex-col items-end ${isPositive ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
      <div className="flex items-center gap-1 font-mono font-medium">
        {isPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
        {formatCurrency(numValue)}
      </div>
      <span className="text-xs font-mono">{formatPercent(numPercent)}</span>
    </div>
  );
}

export default function Positions() {
  const { toast } = useToast();

  const { data: positions, isLoading, refetch } = useQuery<AlpacaPosition[]>({
    queryKey: ["/api/positions"],
  });

  const closePositionMutation = useMutation({
    mutationFn: async (symbol: string) => {
      return apiRequest("DELETE", `/api/positions/${symbol}`);
    },
    onSuccess: (_, symbol) => {
      toast({
        title: "Position Closed",
        description: `Successfully closed position for ${symbol}`,
      });
      queryClient.invalidateQueries({ queryKey: ["/api/positions"] });
      queryClient.invalidateQueries({ queryKey: ["/api/account"] });
      queryClient.invalidateQueries({ queryKey: ["/api/trades"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Close Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const totalMarketValue = positions?.reduce(
    (sum, pos) => sum + parseFloat(pos.marketValue),
    0
  ) || 0;

  const totalUnrealizedPnL = positions?.reduce(
    (sum, pos) => sum + parseFloat(pos.unrealizedPl),
    0
  ) || 0;

  const totalCostBasis = positions?.reduce(
    (sum, pos) => sum + parseFloat(pos.costBasis),
    0
  ) || 0;

  const totalPnLPercent = totalCostBasis > 0 ? totalUnrealizedPnL / totalCostBasis : 0;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold" data-testid="text-page-title">Positions</h1>
          <p className="text-muted-foreground">Manage your open positions</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => refetch()}
          data-testid="button-refresh-positions"
        >
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Card data-testid="card-total-value">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Market Value</CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <div className="text-2xl font-bold font-mono" data-testid="text-total-value">
                {formatCurrency(totalMarketValue)}
              </div>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-total-pnl">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Unrealized P&L</CardTitle>
            {totalUnrealizedPnL >= 0 ? (
              <TrendingUp className="h-4 w-4 text-green-500" />
            ) : (
              <TrendingDown className="h-4 w-4 text-red-500" />
            )}
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <div className={`text-2xl font-bold font-mono ${totalUnrealizedPnL >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`} data-testid="text-total-pnl">
                {formatCurrency(totalUnrealizedPnL)}
              </div>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-total-pnl-percent">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total P&L %</CardTitle>
            <Percent className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <div className={`text-2xl font-bold font-mono ${totalPnLPercent >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`} data-testid="text-total-pnl-percent">
                {formatPercent(totalPnLPercent)}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Briefcase className="h-5 w-5" />
            Open Positions
            {positions && positions.length > 0 && (
              <Badge variant="secondary">{positions.length}</Badge>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3, 4].map((i) => (
                <Skeleton key={i} className="h-16 w-full" />
              ))}
            </div>
          ) : positions && positions.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Symbol</TableHead>
                  <TableHead className="text-right">Quantity</TableHead>
                  <TableHead className="text-right">Avg Entry</TableHead>
                  <TableHead className="text-right">Current Price</TableHead>
                  <TableHead className="text-right">Market Value</TableHead>
                  <TableHead className="text-right">P&L (Today)</TableHead>
                  <TableHead className="text-right">Total P&L</TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {positions.map((position) => {
                  const qty = parseFloat(position.qty);
                  const isLong = qty > 0;
                  return (
                    <TableRow key={position.symbol} data-testid={`row-position-${position.symbol}`}>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <span className="font-medium font-mono">{position.symbol}</span>
                          <Badge variant={isLong ? "secondary" : "outline"} className="text-xs">
                            {isLong ? "LONG" : "SHORT"}
                          </Badge>
                        </div>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {Math.abs(qty)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatCurrency(position.avgEntryPrice)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatCurrency(position.currentPrice)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatCurrency(position.marketValue)}
                      </TableCell>
                      <TableCell className="text-right">
                        <PnLDisplay 
                          value={position.unrealizedIntradayPl} 
                          percent={position.unrealizedIntradayPlpc} 
                        />
                      </TableCell>
                      <TableCell className="text-right">
                        <PnLDisplay 
                          value={position.unrealizedPl} 
                          percent={position.unrealizedPlpc} 
                        />
                      </TableCell>
                      <TableCell className="text-right">
                        <AlertDialog>
                          <AlertDialogTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon"
                              disabled={closePositionMutation.isPending}
                              data-testid={`button-close-position-${position.symbol}`}
                            >
                              <X className="h-4 w-4" />
                            </Button>
                          </AlertDialogTrigger>
                          <AlertDialogContent>
                            <AlertDialogHeader>
                              <AlertDialogTitle>Close Position</AlertDialogTitle>
                              <AlertDialogDescription>
                                Are you sure you want to close your {Math.abs(qty)} share {isLong ? "long" : "short"} position in {position.symbol}? This will create a market order to close the entire position.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Cancel</AlertDialogCancel>
                              <AlertDialogAction
                                onClick={() => closePositionMutation.mutate(position.symbol)}
                              >
                                Close Position
                              </AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : (
            <div className="flex flex-col items-center justify-center py-16 text-center text-muted-foreground">
              <Briefcase className="h-16 w-16 mb-4 opacity-50" />
              <p className="text-lg font-medium">No Open Positions</p>
              <p className="text-sm mt-1">Your portfolio is empty. Positions will appear here after executing trades.</p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
