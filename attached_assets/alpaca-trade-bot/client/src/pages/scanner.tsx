import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import {
  RefreshCw,
  Play,
  Plus,
  Trash2,
  Upload,
  TrendingUp,
  TrendingDown,
  Target,
  Clock,
  List,
  Pencil,
  Check,
  X,
} from "lucide-react";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";

interface Strategy {
  id: string;
  name: string;
  description: string;
  timeframes: string[];
}

interface WatchlistResponse {
  symbols: string[];
  count: number;
}

interface ScanSignal {
  symbol: string;
  strategy: string;
  signalType: string;
  price: number;
}

interface FullScanResult {
  timeframe: string;
  scannedCount: number;
  signalsFound: number;
  signals: ScanSignal[];
  duration: number;
}

interface ScanResult {
  id: number;
  symbol: string;
  strategyId: string;
  strategyName: string;
  signalType: string;
  timeframe: string;
  price: number | null;
  scannedAt: string;
}

interface CustomCondition {
  timeframe: string;
  field: string;
  operator: "==" | "!=" | ">" | "<" | ">=" | "<=";
  value: number | boolean | string;
}

interface CustomStrategyConfig {
  signalType: "LONG" | "SHORT";
  conditions: CustomCondition[];
}

interface CustomStrategy {
  id: string;
  name: string;
  description: string | null;
  isActive: boolean | null;
  conditionsJson: string;
  createdAt: string | null;
  updatedAt: string | null;
}

interface SignalEntry {
  id: string;
  strategyId: string;
  strategyName: string;
  symbol: string;
  signalType: string;
  entryTimeframe: string;
  price: number | null;
  indicatorSnapshot: string | null;
  isActive: boolean;
  exitedAt: string | null;
  exitPrice: number | null;
  notificationSent: boolean;
  createdAt: string;
}

interface FieldOption {
  value: string | number | boolean;
  label: string;
}

interface FieldConfig {
  field: string;
  label: string;
  type: "boolean" | "select" | "number";
  options?: FieldOption[];
}

interface ConfigFieldsResponse {
  fields: FieldConfig[];
  timeframes: string[];
}

function formatCurrency(value: number | null): string {
  if (value === null) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
  }).format(value);
}

function formatTime(dateStr: string): string {
  return new Date(dateStr).toLocaleString();
}

export default function Scanner() {
  const { toast } = useToast();
  const [newSymbol, setNewSymbol] = useState("");
  const [bulkSymbols, setBulkSymbols] = useState("");
  const [selectedTimeframe, setSelectedTimeframe] = useState<string>("all");
  const [scanTimeframes, setScanTimeframes] = useState<string[]>(["15Min", "1Hour"]);

  const { data: strategies, isLoading: strategiesLoading } = useQuery<Strategy[]>({
    queryKey: ["/api/scanner/strategies"],
  });

  const { data: watchlist, isLoading: watchlistLoading } = useQuery<WatchlistResponse>({
    queryKey: ["/api/scanner/watchlist"],
  });

  const { data: scanResults, isLoading: resultsLoading } = useQuery<ScanResult[]>({
    queryKey: ["/api/scanner/results", selectedTimeframe],
  });

  // Custom strategies queries
  const { data: customStrategies, isLoading: customStrategiesLoading } = useQuery<CustomStrategy[]>({
    queryKey: ["/api/scanner/custom-strategies"],
  });

  const { data: configFields } = useQuery<ConfigFieldsResponse>({
    queryKey: ["/api/scanner/config/fields"],
  });

  // Signal entries queries
  const { data: activeSignals, isLoading: activeSignalsLoading } = useQuery<SignalEntry[]>({
    queryKey: ["/api/signals/active"],
    refetchInterval: 30000,
  });

  const { data: recentSignals, isLoading: recentSignalsLoading } = useQuery<SignalEntry[]>({
    queryKey: ["/api/signals/recent"],
    refetchInterval: 30000,
  });

  // Auxiliary matches from other timeframes (for decision support)
  interface AuxiliaryMatchInfo {
    timeframe: string;
    matches: Array<{
      symbol: string;
      strategyId: string;
      strategyName: string;
      signalType: "LONG" | "SHORT";
    }>;
  }
  
  const { data: auxiliaryMatches, isLoading: auxiliaryMatchesLoading } = useQuery<AuxiliaryMatchInfo[]>({
    queryKey: ["/api/signals/auxiliary-matches"],
    refetchInterval: 30000,
  });

  // State for new custom strategy form
  const [newStrategyName, setNewStrategyName] = useState("");
  const [newStrategyDesc, setNewStrategyDesc] = useState("");
  const [newStrategySignal, setNewStrategySignal] = useState<"LONG" | "SHORT">("LONG");
  const [newConditions, setNewConditions] = useState<CustomCondition[]>([
    { timeframe: "15Min", field: "tsiHeikinBullish", operator: "==", value: true }
  ]);

  // State for editing existing strategy
  const [editingStrategyId, setEditingStrategyId] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [editDesc, setEditDesc] = useState("");
  const [editSignal, setEditSignal] = useState<"LONG" | "SHORT">("LONG");
  const [editConditions, setEditConditions] = useState<CustomCondition[]>([]);

  const createCustomStrategyMutation = useMutation({
    mutationFn: async (data: { name: string; description: string; config: CustomStrategyConfig }) => {
      const res = await apiRequest("POST", "/api/scanner/custom-strategies", data);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/custom-strategies"] });
      setNewStrategyName("");
      setNewStrategyDesc("");
      setNewConditions([{ timeframe: "15Min", field: "tsiHeikinBullish", operator: "==", value: true }]);
      toast({ title: "Custom strategy created" });
    },
    onError: () => {
      toast({ title: "Failed to create strategy", variant: "destructive" });
    },
  });

  const deleteCustomStrategyMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await apiRequest("DELETE", `/api/scanner/custom-strategies/${id}`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/custom-strategies"] });
      toast({ title: "Strategy deleted" });
    },
    onError: () => {
      toast({ title: "Failed to delete strategy", variant: "destructive" });
    },
  });

  const updateCustomStrategyMutation = useMutation({
    mutationFn: async (data: { id: string; name: string; description: string; config: CustomStrategyConfig }) => {
      const res = await apiRequest("PATCH", `/api/scanner/custom-strategies/${data.id}`, {
        name: data.name,
        description: data.description,
        config: data.config,
      });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/custom-strategies"] });
      setEditingStrategyId(null);
      toast({ title: "策略已更新" });
    },
    onError: () => {
      toast({ title: "更新策略失败", variant: "destructive" });
    },
  });

  const startEditStrategy = (strategy: CustomStrategy) => {
    let config: CustomStrategyConfig | null = null;
    try {
      config = JSON.parse(strategy.conditionsJson);
    } catch {}
    
    setEditingStrategyId(strategy.id);
    setEditName(strategy.name);
    setEditDesc(strategy.description || "");
    setEditSignal(config?.signalType || "LONG");
    setEditConditions(config?.conditions || []);
  };

  const cancelEdit = () => {
    setEditingStrategyId(null);
    setEditName("");
    setEditDesc("");
    setEditConditions([]);
  };

  const saveEdit = () => {
    if (!editingStrategyId || !editName.trim()) return;
    updateCustomStrategyMutation.mutate({
      id: editingStrategyId,
      name: editName,
      description: editDesc,
      config: {
        signalType: editSignal,
        conditions: editConditions,
      },
    });
  };

  const runCustomStrategiesMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", "/api/scanner/custom-strategies/run");
      return res.json();
    },
    onSuccess: (data) => {
      toast({ title: `Found ${data.length} symbols matching strategies` });
    },
    onError: () => {
      toast({ title: "Failed to run strategies", variant: "destructive" });
    },
  });

  const addSymbolMutation = useMutation({
    mutationFn: async (symbol: string) => {
      const res = await apiRequest("POST", "/api/scanner/watchlist", { symbol });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/watchlist"] });
      setNewSymbol("");
      toast({ title: "Symbol added to watchlist" });
    },
    onError: () => {
      toast({ title: "Failed to add symbol", variant: "destructive" });
    },
  });

  const importSymbolsMutation = useMutation({
    mutationFn: async (symbols: string[]) => {
      const res = await apiRequest("POST", "/api/scanner/watchlist/import", { symbols });
      return res.json() as Promise<{ imported: number }>;
    },
    onSuccess: (data: { imported: number }) => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/watchlist"] });
      setBulkSymbols("");
      toast({ title: `Imported ${data.imported} symbols` });
    },
    onError: () => {
      toast({ title: "Failed to import symbols", variant: "destructive" });
    },
  });

  const removeSymbolMutation = useMutation({
    mutationFn: async (symbol: string) => {
      const res = await apiRequest("DELETE", `/api/scanner/watchlist/${symbol}`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/watchlist"] });
      toast({ title: "Symbol removed from watchlist" });
    },
    onError: () => {
      toast({ title: "Failed to remove symbol", variant: "destructive" });
    },
  });

  const runScanMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", "/api/scanner/scan/full", { 
        timeframes: scanTimeframes, 
        fullRefresh: false 
      });
      return res.json() as Promise<FullScanResult[]>;
    },
    onSuccess: (data: FullScanResult[]) => {
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/results"] });
      const totalSignals = data.reduce((sum, r) => sum + r.signalsFound, 0);
      toast({ 
        title: "Scan complete", 
        description: `Found ${totalSignals} signals across ${data.length} timeframes` 
      });
    },
    onError: () => {
      toast({ title: "Scan failed", variant: "destructive" });
    },
  });

  const handleAddSymbol = (e: React.FormEvent) => {
    e.preventDefault();
    if (newSymbol.trim()) {
      addSymbolMutation.mutate(newSymbol.trim().toUpperCase());
    }
  };

  const handleImportSymbols = () => {
    const symbols = bulkSymbols
      .split(/[\n,]/)
      .map((s) => s.trim().toUpperCase())
      .filter((s) => s.length > 0);
    if (symbols.length > 0) {
      importSymbolsMutation.mutate(symbols);
    }
  };

  const handleTimeframeToggle = (tf: string) => {
    if (scanTimeframes.includes(tf)) {
      setScanTimeframes(scanTimeframes.filter((t) => t !== tf));
    } else {
      setScanTimeframes([...scanTimeframes, tf]);
    }
  };

  const filteredResults = scanResults?.filter((r) =>
    selectedTimeframe === "all" ? true : r.timeframe === selectedTimeframe
  );

  const groupedByStrategy = filteredResults?.reduce((acc, r) => {
    if (!acc[r.strategyName]) {
      acc[r.strategyName] = [];
    }
    acc[r.strategyName].push(r);
    return acc;
  }, {} as Record<string, ScanResult[]>);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold" data-testid="text-page-title">
            Stock Scanner
          </h1>
          <p className="text-muted-foreground">
            Scan stocks for trading signals based on custom indicators
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            onClick={() => runScanMutation.mutate()}
            disabled={runScanMutation.isPending || (watchlist?.count || 0) === 0}
            data-testid="button-run-scan"
          >
            {runScanMutation.isPending ? (
              <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Play className="h-4 w-4 mr-2" />
            )}
            {runScanMutation.isPending ? "Scanning..." : "Run Scan"}
          </Button>
        </div>
      </div>

      <Tabs defaultValue="results" className="w-full">
        <TabsList data-testid="tabs-scanner">
          <TabsTrigger value="results" data-testid="tab-results">
            <Target className="h-4 w-4 mr-2" />
            Scan Results
          </TabsTrigger>
          <TabsTrigger value="watchlist" data-testid="tab-watchlist">
            <List className="h-4 w-4 mr-2" />
            Watchlist ({watchlist?.count || 0})
          </TabsTrigger>
          <TabsTrigger value="strategies" data-testid="tab-strategies">
            <TrendingUp className="h-4 w-4 mr-2" />
            Preset Strategies
          </TabsTrigger>
          <TabsTrigger value="custom-strategies" data-testid="tab-custom-strategies">
            <Target className="h-4 w-4 mr-2" />
            Custom Strategies
          </TabsTrigger>
          <TabsTrigger value="scheduler" data-testid="tab-scheduler">
            <Clock className="h-4 w-4 mr-2" />
            Auto Monitor
          </TabsTrigger>
          <TabsTrigger value="signals" data-testid="tab-signals">
            <TrendingUp className="h-4 w-4 mr-2" />
            Entry Signals
          </TabsTrigger>
        </TabsList>

        <TabsContent value="results" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between gap-4 flex-wrap">
                <div>
                  <CardTitle>Latest Signals</CardTitle>
                  <CardDescription>
                    Stocks matching strategy conditions
                  </CardDescription>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-muted-foreground">Timeframe:</span>
                  <Select value={selectedTimeframe} onValueChange={setSelectedTimeframe}>
                    <SelectTrigger className="w-[140px]" data-testid="select-timeframe-filter">
                      <SelectValue placeholder="All" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All</SelectItem>
                      <SelectItem value="5Min">5 Min</SelectItem>
                      <SelectItem value="15Min">15 Min</SelectItem>
                      <SelectItem value="1Hour">1 Hour</SelectItem>
                      <SelectItem value="4Hour">4 Hour</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={() => queryClient.invalidateQueries({ queryKey: ["/api/scanner/results"] })}
                    data-testid="button-refresh-results"
                  >
                    <RefreshCw className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {resultsLoading ? (
                <div className="space-y-2">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-12 w-full" />
                  ))}
                </div>
              ) : filteredResults && filteredResults.length > 0 ? (
                <div className="space-y-6">
                  {groupedByStrategy && Object.entries(groupedByStrategy).map(([strategyName, results]) => (
                    <div key={strategyName}>
                      <div className="flex items-center gap-2 mb-3">
                        <h3 className="font-medium">{strategyName}</h3>
                        <Badge variant="secondary">{results.length}</Badge>
                      </div>
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Symbol</TableHead>
                            <TableHead>Signal</TableHead>
                            <TableHead>Timeframe</TableHead>
                            <TableHead className="text-right">Price</TableHead>
                            <TableHead className="text-right">Scanned</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {results.map((result) => (
                            <TableRow key={result.id} data-testid={`row-result-${result.id}`}>
                              <TableCell className="font-medium">{result.symbol}</TableCell>
                              <TableCell>
                                <Badge
                                  variant={result.signalType === "LONG" ? "default" : "destructive"}
                                  className="gap-1"
                                >
                                  {result.signalType === "LONG" ? (
                                    <TrendingUp className="h-3 w-3" />
                                  ) : (
                                    <TrendingDown className="h-3 w-3" />
                                  )}
                                  {result.signalType}
                                </Badge>
                              </TableCell>
                              <TableCell>
                                <Badge variant="outline">
                                  <Clock className="h-3 w-3 mr-1" />
                                  {result.timeframe}
                                </Badge>
                              </TableCell>
                              <TableCell className="text-right">
                                {formatCurrency(result.price)}
                              </TableCell>
                              <TableCell className="text-right text-muted-foreground text-sm">
                                {formatTime(result.scannedAt)}
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-12 text-muted-foreground">
                  <Target className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No scan results yet</p>
                  <p className="text-sm mt-2">
                    Add symbols to watchlist and run a scan to find signals
                  </p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Scan Settings</CardTitle>
              <CardDescription>Select timeframes to scan</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {["5Min", "15Min", "1Hour", "4Hour"].map((tf) => (
                  <Button
                    key={tf}
                    variant={scanTimeframes.includes(tf) ? "default" : "outline"}
                    size="sm"
                    onClick={() => handleTimeframeToggle(tf)}
                    data-testid={`button-tf-${tf}`}
                  >
                    {tf}
                  </Button>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="watchlist" className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>Add Symbol</CardTitle>
                <CardDescription>Add individual stock symbols</CardDescription>
              </CardHeader>
              <CardContent>
                <form onSubmit={handleAddSymbol} className="flex gap-2">
                  <Input
                    placeholder="e.g. AAPL"
                    value={newSymbol}
                    onChange={(e) => setNewSymbol(e.target.value.toUpperCase())}
                    className="flex-1"
                    data-testid="input-add-symbol"
                  />
                  <Button
                    type="submit"
                    disabled={addSymbolMutation.isPending || !newSymbol.trim()}
                    data-testid="button-add-symbol"
                  >
                    <Plus className="h-4 w-4 mr-2" />
                    Add
                  </Button>
                </form>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Bulk Import</CardTitle>
                <CardDescription>Paste symbols separated by comma or newline</CardDescription>
              </CardHeader>
              <CardContent>
                <Textarea
                  placeholder="AAPL, GOOGL, MSFT..."
                  value={bulkSymbols}
                  onChange={(e) => setBulkSymbols(e.target.value)}
                  className="mb-2"
                  rows={3}
                  data-testid="textarea-bulk-import"
                />
                <Button
                  onClick={handleImportSymbols}
                  disabled={importSymbolsMutation.isPending || !bulkSymbols.trim()}
                  className="w-full"
                  data-testid="button-bulk-import"
                >
                  <Upload className="h-4 w-4 mr-2" />
                  Import Symbols
                </Button>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Watchlist ({watchlist?.count || 0} symbols)</CardTitle>
              <CardDescription>Stocks being monitored for signals</CardDescription>
            </CardHeader>
            <CardContent>
              {watchlistLoading ? (
                <div className="space-y-2">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-8 w-full" />
                  ))}
                </div>
              ) : watchlist && watchlist.symbols.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {watchlist.symbols.map((symbol) => (
                    <Badge
                      key={symbol}
                      variant="secondary"
                      className="gap-1 pl-3 pr-1 py-1"
                      data-testid={`badge-symbol-${symbol}`}
                    >
                      {symbol}
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-5 w-5 ml-1"
                        onClick={() => removeSymbolMutation.mutate(symbol)}
                        data-testid={`button-remove-${symbol}`}
                      >
                        <Trash2 className="h-3 w-3" />
                      </Button>
                    </Badge>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <List className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No symbols in watchlist</p>
                  <p className="text-sm mt-2">Add symbols above to start scanning</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="strategies" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Available Strategies</CardTitle>
              <CardDescription>
                Predefined condition combinations for signal detection
              </CardDescription>
            </CardHeader>
            <CardContent>
              {strategiesLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              ) : strategies && strategies.length > 0 ? (
                <div className="space-y-4">
                  {strategies.map((strategy) => (
                    <div
                      key={strategy.id}
                      className="p-4 border rounded-md space-y-2"
                      data-testid={`card-strategy-${strategy.id}`}
                    >
                      <div className="flex items-center justify-between gap-4 flex-wrap">
                        <h3 className="font-medium">{strategy.name}</h3>
                        <div className="flex gap-1">
                          {strategy.timeframes.map((tf) => (
                            <Badge key={tf} variant="outline" className="text-xs">
                              {tf}
                            </Badge>
                          ))}
                        </div>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {strategy.description}
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <TrendingUp className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No strategies available</p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Indicator Reference</CardTitle>
              <CardDescription>Custom indicators used for signal detection</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="p-4 border rounded-md">
                  <h4 className="font-medium mb-2">Heikin Ashi TSI</h4>
                  <p className="text-sm text-muted-foreground">
                    True Strength Index with Heikin Ashi candle smoothing. Identifies trend direction 
                    and momentum strength.
                  </p>
                </div>
                <div className="p-4 border rounded-md">
                  <h4 className="font-medium mb-2">Weighted QQE</h4>
                  <p className="text-sm text-muted-foreground">
                    Modified Quantitative Qualitative Estimation with dual timeframe analysis. 
                    Generates buy/sell signals with trend confirmation.
                  </p>
                </div>
                <div className="p-4 border rounded-md">
                  <h4 className="font-medium mb-2">Sinc Momentum</h4>
                  <p className="text-sm text-muted-foreground">
                    Multi-timeframe momentum indicator using sinc function filtering. 
                    Measures trend strength across short, medium, and long periods.
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="custom-strategies" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between gap-4 flex-wrap">
                <div>
                  <CardTitle>Custom Cross-Timeframe Strategies</CardTitle>
                  <CardDescription>
                    Create strategies with conditions across multiple timeframes
                  </CardDescription>
                </div>
                <Button
                  onClick={() => runCustomStrategiesMutation.mutate()}
                  disabled={runCustomStrategiesMutation.isPending || !customStrategies?.length}
                  data-testid="button-run-custom-strategies"
                >
                  {runCustomStrategiesMutation.isPending ? (
                    <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <Play className="h-4 w-4 mr-2" />
                  )}
                  Run All Strategies
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="p-4 border rounded-md space-y-4">
                <h4 className="font-medium">Create New Strategy</h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <Input
                    placeholder="Strategy Name"
                    value={newStrategyName}
                    onChange={(e) => setNewStrategyName(e.target.value)}
                    data-testid="input-strategy-name"
                  />
                  <Select value={newStrategySignal} onValueChange={(v) => setNewStrategySignal(v as "LONG" | "SHORT")}>
                    <SelectTrigger data-testid="select-signal-type">
                      <SelectValue placeholder="Signal Type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="LONG">LONG (Buy)</SelectItem>
                      <SelectItem value="SHORT">SHORT (Sell)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Textarea
                  placeholder="Description (optional)"
                  value={newStrategyDesc}
                  onChange={(e) => setNewStrategyDesc(e.target.value)}
                  className="min-h-[60px]"
                  data-testid="input-strategy-description"
                />
                
                <Separator />
                
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h5 className="text-sm font-medium">Conditions (ALL must match)</h5>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setNewConditions([
                        ...newConditions,
                        { timeframe: "15Min", field: "tsiHeikinBullish", operator: "==", value: true }
                      ])}
                      data-testid="button-add-condition"
                    >
                      <Plus className="h-4 w-4 mr-1" />
                      Add Condition
                    </Button>
                  </div>
                  
                  {newConditions.map((cond, idx) => (
                    <div key={idx} className="flex items-center gap-2 flex-wrap p-2 border rounded-md">
                      <Select 
                        value={cond.timeframe} 
                        onValueChange={(v) => {
                          const updated = [...newConditions];
                          updated[idx].timeframe = v;
                          setNewConditions(updated);
                        }}
                      >
                        <SelectTrigger className="w-[100px]" data-testid={`select-condition-timeframe-${idx}`}>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {(configFields?.timeframes || ["5Min", "15Min", "1Hour", "4Hour"]).map((tf) => (
                            <SelectItem key={tf} value={tf}>{tf}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      
                      <Select 
                        value={cond.field} 
                        onValueChange={(v) => {
                          const updated = [...newConditions];
                          updated[idx].field = v;
                          const fieldConfig = configFields?.fields?.find(f => f.field === v);
                          if (fieldConfig?.options?.length) {
                            updated[idx].value = fieldConfig.options[0].value;
                          }
                          setNewConditions(updated);
                        }}
                      >
                        <SelectTrigger className="w-[180px]" data-testid={`select-condition-field-${idx}`}>
                          <SelectValue placeholder="选择条件" />
                        </SelectTrigger>
                        <SelectContent>
                          {(configFields?.fields || []).map((f) => (
                            <SelectItem key={f.field} value={f.field}>{f.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      
                      <Select 
                        value={cond.operator} 
                        onValueChange={(v) => {
                          const updated = [...newConditions];
                          updated[idx].operator = v as CustomCondition["operator"];
                          setNewConditions(updated);
                        }}
                      >
                        <SelectTrigger className="w-[80px]" data-testid={`select-condition-operator-${idx}`}>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="==">==</SelectItem>
                          <SelectItem value="!=">!=</SelectItem>
                        </SelectContent>
                      </Select>
                      
                      {(() => {
                        const fieldConfig = configFields?.fields?.find(f => f.field === cond.field);
                        if (fieldConfig?.options?.length) {
                          return (
                            <Select 
                              value={String(cond.value)} 
                              onValueChange={(v) => {
                                const updated = [...newConditions];
                                const opt = fieldConfig.options?.find(o => String(o.value) === v);
                                updated[idx].value = opt?.value ?? v;
                                setNewConditions(updated);
                              }}
                            >
                              <SelectTrigger className="w-[150px]" data-testid={`select-condition-value-${idx}`}>
                                <SelectValue placeholder="选择值" />
                              </SelectTrigger>
                              <SelectContent>
                                {fieldConfig.options.map((opt) => (
                                  <SelectItem key={String(opt.value)} value={String(opt.value)}>
                                    {opt.label}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          );
                        }
                        return (
                          <Input
                            className="w-[100px]"
                            value={String(cond.value)}
                            onChange={(e) => {
                              const updated = [...newConditions];
                              const val = e.target.value;
                              if (val === "true") updated[idx].value = true;
                              else if (val === "false") updated[idx].value = false;
                              else if (!isNaN(Number(val)) && val !== "") updated[idx].value = Number(val);
                              else updated[idx].value = val;
                              setNewConditions(updated);
                            }}
                            placeholder="Value"
                            data-testid={`input-condition-value-${idx}`}
                          />
                        );
                      })()}
                      
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => setNewConditions(newConditions.filter((_, i) => i !== idx))}
                        disabled={newConditions.length === 1}
                        data-testid={`button-remove-condition-${idx}`}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  ))}
                </div>
                
                <Button
                  onClick={() => {
                    if (!newStrategyName.trim()) {
                      toast({ title: "Strategy name is required", variant: "destructive" });
                      return;
                    }
                    createCustomStrategyMutation.mutate({
                      name: newStrategyName,
                      description: newStrategyDesc,
                      config: {
                        signalType: newStrategySignal,
                        conditions: newConditions,
                      },
                    });
                  }}
                  disabled={createCustomStrategyMutation.isPending || !newStrategyName.trim()}
                  data-testid="button-create-strategy"
                >
                  {createCustomStrategyMutation.isPending ? (
                    <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <Plus className="h-4 w-4 mr-2" />
                  )}
                  Create Strategy
                </Button>
              </div>
              
              <Separator />
              
              <div>
                <h4 className="font-medium mb-3">Saved Strategies ({customStrategies?.length || 0})</h4>
                {customStrategiesLoading ? (
                  <div className="space-y-2">
                    {[1, 2].map((i) => (
                      <Skeleton key={i} className="h-20" />
                    ))}
                  </div>
                ) : customStrategies && customStrategies.length > 0 ? (
                  <div className="space-y-3">
                    {customStrategies.map((strategy) => {
                      let config: CustomStrategyConfig | null = null;
                      try {
                        config = JSON.parse(strategy.conditionsJson);
                      } catch {}
                      
                      const isEditing = editingStrategyId === strategy.id;
                      
                      if (isEditing) {
                        return (
                          <div 
                            key={strategy.id} 
                            className="p-4 border-2 border-primary rounded-md space-y-4"
                            data-testid={`card-edit-strategy-${strategy.id}`}
                          >
                            <div className="flex items-center justify-between gap-2">
                              <h4 className="font-medium">编辑策略</h4>
                              <div className="flex items-center gap-1">
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  onClick={saveEdit}
                                  disabled={updateCustomStrategyMutation.isPending || !editName.trim()}
                                  data-testid={`button-save-edit-${strategy.id}`}
                                >
                                  {updateCustomStrategyMutation.isPending ? (
                                    <RefreshCw className="h-4 w-4 animate-spin" />
                                  ) : (
                                    <Check className="h-4 w-4 text-green-600" />
                                  )}
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  onClick={cancelEdit}
                                  disabled={updateCustomStrategyMutation.isPending}
                                  data-testid={`button-cancel-edit-${strategy.id}`}
                                >
                                  <X className="h-4 w-4 text-red-600" />
                                </Button>
                              </div>
                            </div>
                            
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                              <Input
                                placeholder="策略名称"
                                value={editName}
                                onChange={(e) => setEditName(e.target.value)}
                                data-testid={`input-edit-name-${strategy.id}`}
                              />
                              <Select value={editSignal} onValueChange={(v) => setEditSignal(v as "LONG" | "SHORT")}>
                                <SelectTrigger data-testid={`select-edit-signal-${strategy.id}`}>
                                  <SelectValue placeholder="信号类型" />
                                </SelectTrigger>
                                <SelectContent>
                                  <SelectItem value="LONG">LONG (做多)</SelectItem>
                                  <SelectItem value="SHORT">SHORT (做空)</SelectItem>
                                </SelectContent>
                              </Select>
                            </div>
                            <Input
                              placeholder="描述 (可选)"
                              value={editDesc}
                              onChange={(e) => setEditDesc(e.target.value)}
                              data-testid={`input-edit-desc-${strategy.id}`}
                            />
                            
                            <div className="space-y-3">
                              <div className="flex items-center justify-between">
                                <h5 className="text-sm font-medium">条件 (全部满足)</h5>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => setEditConditions([
                                    ...editConditions,
                                    { timeframe: "15Min", field: "tsiHeikinBullish", operator: "==", value: true }
                                  ])}
                                  data-testid={`button-add-edit-condition-${strategy.id}`}
                                >
                                  <Plus className="h-4 w-4 mr-1" />
                                  添加条件
                                </Button>
                              </div>
                              
                              {editConditions.map((cond, idx) => (
                                <div key={idx} className="flex items-center gap-2 flex-wrap p-2 border rounded-md">
                                  <Select 
                                    value={cond.timeframe} 
                                    onValueChange={(v) => {
                                      const updated = [...editConditions];
                                      updated[idx].timeframe = v;
                                      setEditConditions(updated);
                                    }}
                                  >
                                    <SelectTrigger className="w-[100px]">
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      {(configFields?.timeframes || ["5Min", "15Min", "1Hour", "4Hour"]).map((tf) => (
                                        <SelectItem key={tf} value={tf}>{tf}</SelectItem>
                                      ))}
                                    </SelectContent>
                                  </Select>
                                  
                                  <Select 
                                    value={cond.field} 
                                    onValueChange={(v) => {
                                      const updated = [...editConditions];
                                      updated[idx].field = v;
                                      const fieldConfig = configFields?.fields?.find(f => f.field === v);
                                      if (fieldConfig?.options?.length) {
                                        updated[idx].value = fieldConfig.options[0].value;
                                      }
                                      setEditConditions(updated);
                                    }}
                                  >
                                    <SelectTrigger className="w-[180px]">
                                      <SelectValue placeholder="选择条件" />
                                    </SelectTrigger>
                                    <SelectContent>
                                      {(configFields?.fields || []).map((f) => (
                                        <SelectItem key={f.field} value={f.field}>{f.label}</SelectItem>
                                      ))}
                                    </SelectContent>
                                  </Select>
                                  
                                  <Select 
                                    value={cond.operator} 
                                    onValueChange={(v) => {
                                      const updated = [...editConditions];
                                      updated[idx].operator = v as CustomCondition["operator"];
                                      setEditConditions(updated);
                                    }}
                                  >
                                    <SelectTrigger className="w-[80px]">
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="==">==</SelectItem>
                                      <SelectItem value="!=">!=</SelectItem>
                                    </SelectContent>
                                  </Select>
                                  
                                  {(() => {
                                    const fieldConfig = configFields?.fields?.find(f => f.field === cond.field);
                                    if (fieldConfig?.options?.length) {
                                      return (
                                        <Select 
                                          value={String(cond.value)} 
                                          onValueChange={(v) => {
                                            const updated = [...editConditions];
                                            const opt = fieldConfig.options?.find(o => String(o.value) === v);
                                            updated[idx].value = opt?.value ?? v;
                                            setEditConditions(updated);
                                          }}
                                        >
                                          <SelectTrigger className="w-[150px]">
                                            <SelectValue placeholder="选择值" />
                                          </SelectTrigger>
                                          <SelectContent>
                                            {fieldConfig.options.map((opt) => (
                                              <SelectItem key={String(opt.value)} value={String(opt.value)}>
                                                {opt.label}
                                              </SelectItem>
                                            ))}
                                          </SelectContent>
                                        </Select>
                                      );
                                    }
                                    return (
                                      <Input
                                        className="w-[100px]"
                                        value={String(cond.value)}
                                        onChange={(e) => {
                                          const updated = [...editConditions];
                                          const val = e.target.value;
                                          if (val === "true") updated[idx].value = true;
                                          else if (val === "false") updated[idx].value = false;
                                          else if (!isNaN(Number(val)) && val !== "") updated[idx].value = Number(val);
                                          else updated[idx].value = val;
                                          setEditConditions(updated);
                                        }}
                                        placeholder="Value"
                                      />
                                    );
                                  })()}
                                  
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    onClick={() => setEditConditions(editConditions.filter((_, i) => i !== idx))}
                                    disabled={editConditions.length === 1}
                                  >
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      }
                      
                      return (
                        <div 
                          key={strategy.id} 
                          className="p-4 border rounded-md"
                          data-testid={`card-custom-strategy-${strategy.id}`}
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="flex-1">
                              <div className="flex items-center gap-2 mb-1">
                                <h4 className="font-medium">{strategy.name}</h4>
                                <Badge variant={config?.signalType === "LONG" ? "default" : "secondary"}>
                                  {config?.signalType || "?"}
                                </Badge>
                                {strategy.isActive === false && (
                                  <Badge variant="outline">Inactive</Badge>
                                )}
                              </div>
                              {strategy.description && (
                                <p className="text-sm text-muted-foreground mb-2">
                                  {strategy.description}
                                </p>
                              )}
                              {config && (
                                <div className="flex flex-wrap gap-1 mt-2">
                                  {config.conditions.map((c, i) => {
                                    const fieldConfig = configFields?.fields?.find(f => f.field === c.field);
                                    const valueLabel = fieldConfig?.options?.find(o => o.value === c.value)?.label || String(c.value);
                                    return (
                                      <Badge key={i} variant="outline" className="text-xs">
                                        {c.timeframe}: {fieldConfig?.label || c.field} {c.operator} {valueLabel}
                                      </Badge>
                                    );
                                  })}
                                </div>
                              )}
                            </div>
                            <div className="flex items-center gap-1">
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => startEditStrategy(strategy)}
                                disabled={editingStrategyId !== null}
                                data-testid={`button-edit-strategy-${strategy.id}`}
                              >
                                <Pencil className="h-4 w-4" />
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => deleteCustomStrategyMutation.mutate(strategy.id)}
                                disabled={deleteCustomStrategyMutation.isPending || editingStrategyId !== null}
                                data-testid={`button-delete-strategy-${strategy.id}`}
                              >
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    <Target className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No custom strategies yet</p>
                    <p className="text-sm mt-1">Create your first cross-timeframe strategy above</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="scheduler" className="space-y-4">
          <SchedulerTab />
        </TabsContent>

        <TabsContent value="signals" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between gap-4 flex-wrap">
                <div>
                  <CardTitle>Active Entry Signals</CardTitle>
                  <CardDescription>
                    First-time matches indicating entry opportunities
                  </CardDescription>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    queryClient.invalidateQueries({ queryKey: ["/api/signals/active"] });
                    queryClient.invalidateQueries({ queryKey: ["/api/signals/recent"] });
                    queryClient.invalidateQueries({ queryKey: ["/api/signals/auxiliary-matches"] });
                  }}
                  data-testid="button-refresh-signals"
                >
                  <RefreshCw className="h-4 w-4 mr-2" />
                  Refresh
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {activeSignalsLoading ? (
                <Skeleton className="h-40 w-full" />
              ) : activeSignals && activeSignals.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Signal</TableHead>
                      <TableHead>Strategy</TableHead>
                      <TableHead>Timeframe</TableHead>
                      <TableHead>Price</TableHead>
                      <TableHead>Entry Time</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {activeSignals.map((signal) => (
                      <TableRow key={signal.id} data-testid={`row-signal-${signal.id}`}>
                        <TableCell className="font-medium">{signal.symbol}</TableCell>
                        <TableCell>
                          <Badge variant={signal.signalType === "LONG" ? "default" : "destructive"}>
                            {signal.signalType === "LONG" ? (
                              <TrendingUp className="h-3 w-3 mr-1" />
                            ) : (
                              <TrendingDown className="h-3 w-3 mr-1" />
                            )}
                            {signal.signalType}
                          </Badge>
                        </TableCell>
                        <TableCell>{signal.strategyName}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{signal.entryTimeframe}</Badge>
                        </TableCell>
                        <TableCell>{formatCurrency(signal.price)}</TableCell>
                        <TableCell>{formatTime(signal.createdAt)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <TrendingUp className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No active entry signals</p>
                  <p className="text-sm mt-1">Entry signals will appear when strategies first match</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Auxiliary Matches from Other Timeframes */}
          <Card data-testid="card-auxiliary-matches">
            <CardHeader>
              <CardTitle>Other Timeframes Reference</CardTitle>
              <CardDescription>
                Current matches from other timeframes (for decision support, not triggering new entries)
              </CardDescription>
            </CardHeader>
            <CardContent>
              {auxiliaryMatchesLoading ? (
                <Skeleton className="h-24 w-full" />
              ) : auxiliaryMatches && auxiliaryMatches.length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                  {auxiliaryMatches.map((tf) => (
                    <Card key={tf.timeframe} className="p-3" data-testid={`card-aux-tf-${tf.timeframe}`}>
                      <div className="flex items-center gap-2 mb-2">
                        <Badge variant="outline">{tf.timeframe}</Badge>
                        <span className="text-xs text-muted-foreground" data-testid={`text-aux-count-${tf.timeframe}`}>{tf.matches.length} matches</span>
                      </div>
                      {tf.matches.length > 0 ? (
                        <div className="space-y-1 max-h-40 overflow-y-auto">
                          {tf.matches.map((m, idx) => (
                            <div key={`${m.symbol}-${m.strategyId}-${idx}`} className="flex items-center justify-between text-sm py-1 border-b last:border-0" data-testid={`row-aux-${tf.timeframe}-${m.symbol}-${idx}`}>
                              <span className="font-medium" data-testid={`text-aux-symbol-${tf.timeframe}-${idx}`}>{m.symbol}</span>
                              <Badge 
                                variant={m.signalType === "LONG" ? "default" : "destructive"}
                                data-testid={`badge-aux-signal-${tf.timeframe}-${idx}`}
                              >
                                {m.signalType === "LONG" ? (
                                  <TrendingUp className="h-3 w-3 mr-1" />
                                ) : (
                                  <TrendingDown className="h-3 w-3 mr-1" />
                                )}
                                {m.signalType}
                              </Badge>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <p className="text-xs text-muted-foreground">No matches</p>
                      )}
                    </Card>
                  ))}
                </div>
              ) : (
                <div className="text-center py-4 text-muted-foreground">
                  <p className="text-sm">No auxiliary timeframe data available</p>
                  <p className="text-xs mt-1">Run a scan to see matches from other timeframes</p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Recent Signal History</CardTitle>
              <CardDescription>
                All entry signals including exited positions
              </CardDescription>
            </CardHeader>
            <CardContent>
              {recentSignalsLoading ? (
                <Skeleton className="h-40 w-full" />
              ) : recentSignals && recentSignals.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Signal</TableHead>
                      <TableHead>Strategy</TableHead>
                      <TableHead>Entry Price</TableHead>
                      <TableHead>Entry Time</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Exit</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {recentSignals.map((signal) => (
                      <TableRow key={signal.id} data-testid={`row-history-${signal.id}`}>
                        <TableCell className="font-medium">{signal.symbol}</TableCell>
                        <TableCell>
                          <Badge variant={signal.signalType === "LONG" ? "default" : "destructive"}>
                            {signal.signalType}
                          </Badge>
                        </TableCell>
                        <TableCell>{signal.strategyName}</TableCell>
                        <TableCell>{formatCurrency(signal.price)}</TableCell>
                        <TableCell>{formatTime(signal.createdAt)}</TableCell>
                        <TableCell>
                          {signal.isActive ? (
                            <Badge variant="default">Active</Badge>
                          ) : (
                            <Badge variant="secondary">Exited</Badge>
                          )}
                        </TableCell>
                        <TableCell>
                          {signal.exitedAt ? (
                            <span className="text-sm">
                              {formatCurrency(signal.exitPrice)} @ {formatTime(signal.exitedAt)}
                            </span>
                          ) : (
                            "-"
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <Clock className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No signal history yet</p>
                  <p className="text-sm mt-1">Start the Auto Monitor to generate entry signals</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function SchedulerTab() {
  const { toast } = useToast();

  interface SchedulerStatus {
    isRunning: boolean;
    marketOpen: boolean;
    lastRun: Record<string, string | null>;
    nextRun: Record<string, string | null>;
    results: Record<string, { matches: number; timestamp: string }>;
    errors: Record<string, string>;
    schedules: Record<string, string>;
  }

  const { data: status, isLoading } = useQuery<SchedulerStatus>({
    queryKey: ["/api/scheduler/status"],
    refetchInterval: 10000,
  });

  const startMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", "/api/scheduler/start");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scheduler/status"] });
      toast({ title: "Scheduler started" });
    },
  });

  const stopMutation = useMutation({
    mutationFn: async () => {
      const res = await apiRequest("POST", "/api/scheduler/stop");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scheduler/status"] });
      toast({ title: "Scheduler stopped" });
    },
  });

  const manualScanMutation = useMutation({
    mutationFn: async (timeframe?: string) => {
      const res = await apiRequest("POST", "/api/scheduler/scan", { timeframe });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/scheduler/status"] });
      queryClient.invalidateQueries({ queryKey: ["/api/scanner/results"] });
      toast({ title: "Scan completed" });
    },
  });

  const timeframes = ["5Min", "15Min", "1Hour", "4Hour"];

  if (isLoading) {
    return (
      <Card>
        <CardContent className="py-8">
          <Skeleton className="h-40 w-full" />
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <CardTitle className="flex items-center gap-2">
                <Clock className="h-5 w-5" />
                Auto Monitor
              </CardTitle>
              <CardDescription>
                Automatically fetch K-lines, calculate indicators, and run strategies
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant={status?.marketOpen ? "default" : "secondary"}>
                {status?.marketOpen ? "Market Open" : "Market Closed"}
              </Badge>
              <Badge variant={status?.isRunning ? "default" : "outline"}>
                {status?.isRunning ? "Running" : "Stopped"}
              </Badge>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="flex gap-2 mb-6">
            {status?.isRunning ? (
              <Button
                variant="destructive"
                onClick={() => stopMutation.mutate()}
                disabled={stopMutation.isPending}
                data-testid="button-stop-scheduler"
              >
                <X className="h-4 w-4 mr-2" />
                Stop Scheduler
              </Button>
            ) : (
              <Button
                onClick={() => startMutation.mutate()}
                disabled={startMutation.isPending}
                data-testid="button-start-scheduler"
              >
                <Play className="h-4 w-4 mr-2" />
                Start Scheduler
              </Button>
            )}
            <Button
              variant="outline"
              onClick={() => manualScanMutation.mutate(undefined)}
              disabled={manualScanMutation.isPending}
              data-testid="button-manual-scan-all"
            >
              <RefreshCw className={`h-4 w-4 mr-2 ${manualScanMutation.isPending ? "animate-spin" : ""}`} />
              Scan All Now
            </Button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {timeframes.map((tf) => (
              <Card key={tf} className="bg-muted/30">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium flex items-center justify-between">
                    <span>{tf}</span>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => manualScanMutation.mutate(tf)}
                      disabled={manualScanMutation.isPending}
                      data-testid={`button-scan-${tf}`}
                    >
                      <RefreshCw className="h-4 w-4" />
                    </Button>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Schedule:</span>
                    <span>{status?.schedules?.[tf]?.split("(")[1]?.replace(")", "") || "-"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Last Run:</span>
                    <span>
                      {status?.lastRun?.[tf]
                        ? new Date(status.lastRun[tf]!).toLocaleTimeString()
                        : "-"}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Matches:</span>
                    <Badge variant="outline">
                      {status?.results?.[tf]?.matches || 0}
                    </Badge>
                  </div>
                  {status?.errors?.[tf] && (
                    <div className="text-destructive text-xs mt-2">
                      Error: {status.errors[tf]}
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Schedule Details</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Timeframe</TableHead>
                <TableHead>Schedule</TableHead>
                <TableHead>Description</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TableRow>
                <TableCell className="font-medium">5Min</TableCell>
                <TableCell><code>*/5 * * * *</code></TableCell>
                <TableCell>Every 5 minutes during market hours</TableCell>
              </TableRow>
              <TableRow>
                <TableCell className="font-medium">15Min</TableCell>
                <TableCell><code>*/15 * * * *</code></TableCell>
                <TableCell>Every 15 minutes during market hours</TableCell>
              </TableRow>
              <TableRow>
                <TableCell className="font-medium">1Hour</TableCell>
                <TableCell><code>0 * * * *</code></TableCell>
                <TableCell>Every hour at :00 during market hours</TableCell>
              </TableRow>
              <TableRow>
                <TableCell className="font-medium">4Hour</TableCell>
                <TableCell><code>0 */4 * * *</code></TableCell>
                <TableCell>Every 4 hours at :00 during market hours</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
