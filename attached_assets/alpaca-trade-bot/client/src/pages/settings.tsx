import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Settings as SettingsIcon,
  Key,
  Shield,
  Bell,
  Save,
  RefreshCw,
  Copy,
  CheckCircle,
  AlertCircle,
  Webhook,
} from "lucide-react";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import type { AlpacaAccount } from "@shared/schema";

interface TradingConfig {
  tradingEnabled: boolean;
  maxTradeAmount: number;
  webhookUrl: string;
}

export default function Settings() {
  const { toast } = useToast();
  const [copied, setCopied] = useState(false);

  const { data: account, isLoading: accountLoading, refetch: refetchAccount } = useQuery<AlpacaAccount>({
    queryKey: ["/api/account"],
  });

  const { data: config, isLoading: configLoading } = useQuery<TradingConfig>({
    queryKey: ["/api/config"],
  });

  const [tradingEnabled, setTradingEnabled] = useState(config?.tradingEnabled ?? true);
  const [maxTradeAmount, setMaxTradeAmount] = useState(config?.maxTradeAmount?.toString() ?? "100000");

  const updateConfigMutation = useMutation({
    mutationFn: async (data: Partial<TradingConfig>) => {
      return apiRequest("PATCH", "/api/config", data);
    },
    onSuccess: () => {
      toast({
        title: "Settings Saved",
        description: "Your trading configuration has been updated.",
      });
      queryClient.invalidateQueries({ queryKey: ["/api/config"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Save Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const resetAccountMutation = useMutation({
    mutationFn: async () => {
      return apiRequest("POST", "/api/account/reset");
    },
    onSuccess: () => {
      toast({
        title: "Account Reset",
        description: "Your paper trading account has been reset.",
      });
      queryClient.invalidateQueries({ queryKey: ["/api/account"] });
      queryClient.invalidateQueries({ queryKey: ["/api/positions"] });
      queryClient.invalidateQueries({ queryKey: ["/api/trades"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Reset Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleSaveConfig = () => {
    updateConfigMutation.mutate({
      tradingEnabled,
      maxTradeAmount: parseFloat(maxTradeAmount),
    });
  };

  const webhookUrl = typeof window !== "undefined" 
    ? `${window.location.origin}/api/webhook`
    : "/api/webhook";

  const copyWebhookUrl = () => {
    navigator.clipboard.writeText(webhookUrl);
    setCopied(true);
    toast({
      title: "Copied",
      description: "Webhook URL copied to clipboard",
    });
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold" data-testid="text-page-title">Settings</h1>
        <p className="text-muted-foreground">Configure your trading bot and API settings</p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card data-testid="card-account-info">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Key className="h-5 w-5" />
              Account Information
            </CardTitle>
            <CardDescription>Your Alpaca paper trading account details</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {accountLoading ? (
              <div className="space-y-3">
                {[1, 2, 3, 4].map((i) => (
                  <Skeleton key={i} className="h-6 w-full" />
                ))}
              </div>
            ) : account ? (
              <>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Account ID</span>
                  <span className="font-mono text-sm" data-testid="text-account-id">{account.id?.slice(0, 12)}...</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Account Number</span>
                  <span className="font-mono text-sm" data-testid="text-account-number">{account.accountNumber}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Status</span>
                  <Badge 
                    variant={account.status === "ACTIVE" ? "secondary" : "destructive"}
                    className={account.status === "ACTIVE" ? "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400" : ""}
                    data-testid="text-account-status"
                  >
                    {account.status}
                  </Badge>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Currency</span>
                  <span className="font-mono text-sm">{account.currency}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Multiplier</span>
                  <span className="font-mono text-sm">{account.multiplier}x</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Pattern Day Trader</span>
                  {account.patternDayTrader ? (
                    <Badge variant="secondary">Yes</Badge>
                  ) : (
                    <Badge variant="outline">No</Badge>
                  )}
                </div>
                <Separator />
                <div className="flex items-center justify-between gap-4">
                  <div className="space-y-1">
                    <p className="text-sm font-medium">Account Blocks</p>
                    <p className="text-xs text-muted-foreground">Current trading restrictions</p>
                  </div>
                  <div className="flex gap-2 flex-wrap justify-end">
                    {account.tradingBlocked && (
                      <Badge variant="destructive">Trading Blocked</Badge>
                    )}
                    {account.transfersBlocked && (
                      <Badge variant="destructive">Transfers Blocked</Badge>
                    )}
                    {account.accountBlocked && (
                      <Badge variant="destructive">Account Blocked</Badge>
                    )}
                    {!account.tradingBlocked && !account.transfersBlocked && !account.accountBlocked && (
                      <Badge variant="secondary" className="bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400">
                        <CheckCircle className="h-3 w-3 mr-1" />
                        No Restrictions
                      </Badge>
                    )}
                  </div>
                </div>
              </>
            ) : (
              <div className="flex flex-col items-center py-6 text-center text-muted-foreground">
                <AlertCircle className="h-10 w-10 mb-2 text-destructive" />
                <p className="font-medium">Account not connected</p>
                <p className="text-sm">Please configure your Alpaca API keys</p>
              </div>
            )}
          </CardContent>
        </Card>

        <Card data-testid="card-webhook">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Webhook className="h-5 w-5" />
              Webhook Configuration
            </CardTitle>
            <CardDescription>Use this URL in TradingView to send signals</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="webhook-url">Webhook URL</Label>
              <div className="flex gap-2">
                <Input
                  id="webhook-url"
                  value={webhookUrl}
                  readOnly
                  className="font-mono text-sm"
                  data-testid="input-webhook-url"
                />
                <Button
                  variant="outline"
                  size="icon"
                  onClick={copyWebhookUrl}
                  data-testid="button-copy-webhook"
                >
                  {copied ? <CheckCircle className="h-4 w-4 text-green-500" /> : <Copy className="h-4 w-4" />}
                </Button>
              </div>
            </div>
            <div className="rounded-md bg-muted p-4 text-sm">
              <p className="font-medium mb-2">TradingView Signal Format:</p>
              <pre className="text-xs overflow-x-auto font-mono bg-background p-2 rounded">
{`{
  "symbol": "AAPL",
  "side": "buy",
  "quantity": 10,
  "order_type": "market",
  "time_in_force": "day"
}`}
              </pre>
            </div>
          </CardContent>
        </Card>

        <Card data-testid="card-trading-config">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Shield className="h-5 w-5" />
              Trading Configuration
            </CardTitle>
            <CardDescription>Configure trading limits and behavior</CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <Label htmlFor="trading-enabled">Trading Enabled</Label>
                <p className="text-xs text-muted-foreground">
                  Enable or disable automated trading
                </p>
              </div>
              <Switch
                id="trading-enabled"
                checked={tradingEnabled}
                onCheckedChange={setTradingEnabled}
                data-testid="switch-trading-enabled"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="max-trade-amount">Max Trade Amount ($)</Label>
              <Input
                id="max-trade-amount"
                type="number"
                value={maxTradeAmount}
                onChange={(e) => setMaxTradeAmount(e.target.value)}
                min="0"
                step="1000"
                data-testid="input-max-trade-amount"
              />
              <p className="text-xs text-muted-foreground">
                Maximum dollar amount per trade
              </p>
            </div>

            <Button
              onClick={handleSaveConfig}
              disabled={updateConfigMutation.isPending}
              className="w-full"
              data-testid="button-save-config"
            >
              {updateConfigMutation.isPending ? (
                <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Save className="h-4 w-4 mr-2" />
              )}
              Save Configuration
            </Button>
          </CardContent>
        </Card>

        <Card data-testid="card-notifications">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Bell className="h-5 w-5" />
              Quick Actions
            </CardTitle>
            <CardDescription>Account management actions</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <p className="text-sm font-medium">Refresh Account Data</p>
              <p className="text-xs text-muted-foreground mb-2">
                Fetch the latest account information from Alpaca
              </p>
              <Button
                variant="outline"
                onClick={() => refetchAccount()}
                className="w-full"
                data-testid="button-refresh-account"
              >
                <RefreshCw className="h-4 w-4 mr-2" />
                Refresh Account
              </Button>
            </div>

            <Separator />

            <div className="space-y-2">
              <p className="text-sm font-medium text-destructive">Reset Paper Account</p>
              <p className="text-xs text-muted-foreground mb-2">
                Reset your paper trading account to start fresh. This will clear all positions and order history.
              </p>
              <Button
                variant="destructive"
                onClick={() => resetAccountMutation.mutate()}
                disabled={resetAccountMutation.isPending}
                className="w-full"
                data-testid="button-reset-account"
              >
                {resetAccountMutation.isPending ? (
                  <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4 mr-2" />
                )}
                Reset Paper Account
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
