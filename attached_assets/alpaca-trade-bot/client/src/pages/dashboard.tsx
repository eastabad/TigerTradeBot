import { useQuery } from "@tanstack/react-query";
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
import {
  DollarSign,
  TrendingUp,
  TrendingDown,
  Briefcase,
  RefreshCw,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { queryClient } from "@/lib/queryClient";
import type { AlpacaAccount, AlpacaPosition } from "@shared/schema";

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

export default function Dashboard() {
  const { data: accounts, isLoading: accountsLoading } = useQuery<AccountInfo[]>({
    queryKey: ["/api/accounts"],
  });

  const { data: positionsByAccount, isLoading: positionsLoading } = useQuery<PositionsByAccount[]>({
    queryKey: ["/api/positions/by-account"],
  });

  const initialCapitalPerAccount = 100000;
  const totalInitialCapital = initialCapitalPerAccount * 3;

  const totalPortfolioValue = accounts?.reduce((sum, acc) => 
    sum + parseFloat(acc.account.portfolioValue), 0) || 0;
  
  const totalDayChange = accounts?.reduce((sum, acc) => {
    const current = parseFloat(acc.account.portfolioValue);
    const last = parseFloat(acc.account.lastEquity);
    return sum + (current - last);
  }, 0) || 0;

  const totalReturn = totalPortfolioValue - totalInitialCapital;
  const totalReturnPercent = totalReturn / totalInitialCapital;

  const allPositions = positionsByAccount?.flatMap(p => p.positions) || [];
  const totalUnrealizedPl = allPositions.reduce((sum, pos) => sum + parseFloat(pos.unrealizedPl), 0);
  const totalCostBasis = allPositions.reduce((sum, pos) => sum + parseFloat(pos.costBasis), 0);
  const totalPlPercent = totalCostBasis > 0 ? (totalUnrealizedPl / totalCostBasis) : 0;

  const handleRefresh = () => {
    queryClient.invalidateQueries({ queryKey: ["/api/accounts"] });
    queryClient.invalidateQueries({ queryKey: ["/api/positions/by-account"] });
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold" data-testid="text-page-title">Multi-Account Trading Bot</h1>
          <p className="text-muted-foreground">Combined portfolio overview</p>
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
        <Card data-testid="card-total-portfolio">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Portfolio</CardTitle>
            <DollarSign className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {accountsLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <>
                <div className="text-2xl font-bold font-mono" data-testid="text-total-portfolio">
                  {formatCurrency(totalPortfolioValue)}
                </div>
                <p className={`text-xs flex items-center gap-1 ${totalReturn >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                  {totalReturn >= 0 ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                  {formatCurrency(totalReturn)} ({formatPercent(totalReturnPercent)}) total
                </p>
              </>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-total-positions">
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Open Positions</CardTitle>
            <Briefcase className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {positionsLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <>
                <div className="text-2xl font-bold font-mono" data-testid="text-position-count">
                  {allPositions.length}
                </div>
                <p className="text-xs text-muted-foreground">
                  Across {accounts?.length || 0} accounts
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
            {totalDayChange >= 0 ? (
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
                <div className={`text-2xl font-bold font-mono ${totalDayChange >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`} data-testid="text-day-change">
                  {formatCurrency(totalDayChange)}
                </div>
                <p className="text-xs text-muted-foreground">
                  Combined today
                </p>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        {accountsLoading ? (
          [1, 2, 3].map((i) => (
            <Card key={i}>
              <CardHeader>
                <Skeleton className="h-5 w-40" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-20 w-full" />
              </CardContent>
            </Card>
          ))
        ) : (
          accounts?.map((acc) => {
            const portfolioValue = parseFloat(acc.account.portfolioValue);
            const lastEquity = parseFloat(acc.account.lastEquity);
            const dayChange = portfolioValue - lastEquity;
            const returnValue = portfolioValue - initialCapitalPerAccount;
            const returnPercent = returnValue / initialCapitalPerAccount;
            const positions = positionsByAccount?.find(p => p.accountId === acc.accountId)?.positions || [];
            
            return (
              <Card key={acc.accountId} data-testid={`card-account-${acc.accountId}`}>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base flex items-center justify-between gap-2 flex-wrap">
                    <span>{acc.name}</span>
                    <Badge variant="outline" className="font-mono text-xs">
                      {acc.account.accountNumber}
                    </Badge>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex justify-between items-baseline gap-2">
                    <span className="text-muted-foreground text-sm">Portfolio</span>
                    <span className="font-mono font-semibold">{formatCurrency(portfolioValue)}</span>
                  </div>
                  <div className="flex justify-between items-baseline gap-2">
                    <span className="text-muted-foreground text-sm">Day Change</span>
                    <span className={`font-mono ${dayChange >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                      {formatCurrency(dayChange)}
                    </span>
                  </div>
                  <div className="flex justify-between items-baseline gap-2">
                    <span className="text-muted-foreground text-sm">Total Return</span>
                    <span className={`font-mono ${returnValue >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                      {formatPercent(returnPercent)}
                    </span>
                  </div>
                  <div className="flex justify-between items-baseline gap-2">
                    <span className="text-muted-foreground text-sm">Positions</span>
                    <span className="font-mono">{positions.length}</span>
                  </div>
                  <div className="pt-2 border-t">
                    <p className="text-xs text-muted-foreground">
                      Symbols: {acc.symbols.length > 0 ? acc.symbols.join(", ") : "All other"}
                    </p>
                  </div>
                </CardContent>
              </Card>
            );
          })
        )}
      </div>

      <Card data-testid="card-positions">
        <CardHeader className="flex flex-row items-center justify-between gap-2">
          <CardTitle className="flex items-center gap-2">
            <Briefcase className="h-5 w-5" />
            All Positions
          </CardTitle>
        </CardHeader>
        <CardContent>
          {positionsLoading ? (
            <div className="space-y-3">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : allPositions.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Account</TableHead>
                  <TableHead>Symbol</TableHead>
                  <TableHead className="text-right">Qty</TableHead>
                  <TableHead className="text-right">Avg Cost</TableHead>
                  <TableHead className="text-right">Current</TableHead>
                  <TableHead className="text-right">P&L</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {positionsByAccount?.map((accPositions) => 
                  accPositions.positions.map((pos) => {
                    const pnl = parseFloat(pos.unrealizedPl);
                    const pnlPercent = parseFloat(pos.unrealizedPlpc);
                    return (
                      <TableRow key={`${accPositions.accountId}-${pos.symbol}`} data-testid={`row-position-${pos.symbol}`}>
                        <TableCell>
                          <Badge variant="secondary" className="text-xs">
                            {accPositions.accountName.replace("Strategy ", "S")}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-medium font-mono">{pos.symbol}</TableCell>
                        <TableCell className="text-right font-mono">{pos.qty}</TableCell>
                        <TableCell className="text-right font-mono">{formatCurrency(pos.avgEntryPrice)}</TableCell>
                        <TableCell className="text-right font-mono">{formatCurrency(pos.currentPrice)}</TableCell>
                        <TableCell className={`text-right font-mono ${pnl >= 0 ? "text-green-600 dark:text-green-400" : "text-red-600 dark:text-red-400"}`}>
                          {formatCurrency(pnl)}
                          <span className="text-xs ml-1">({formatPercent(pnlPercent)})</span>
                        </TableCell>
                      </TableRow>
                    );
                  })
                )}
              </TableBody>
            </Table>
          ) : (
            <div className="flex flex-col items-center justify-center py-8 text-center text-muted-foreground">
              <Briefcase className="h-12 w-12 mb-3 opacity-50" />
              <p className="font-medium">No positions</p>
              <p className="text-sm">Your open positions will appear here</p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
