import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { TrendingUp } from "lucide-react";

const backtestData = [
  {
    symbol: "TSLL",
    strategies: [
      { timeframe: "15min", days: 365, return: 490.4 },
      { timeframe: "5min", days: 150, return: 226.11 },
    ],
  },
  {
    symbol: "SOXL",
    strategies: [
      { timeframe: "15min", days: 365, return: 241.93 },
      { timeframe: "5min", days: 150, return: 231.21 },
    ],
  },
];

const chartData15min = [
  { name: "TSLL", return: 490.4, fill: "hsl(var(--chart-1))" },
  { name: "SOXL", return: 241.93, fill: "hsl(var(--chart-2))" },
];

const chartData5min = [
  { name: "TSLL", return: 226.11, fill: "hsl(var(--chart-1))" },
  { name: "SOXL", return: 231.21, fill: "hsl(var(--chart-2))" },
];

export default function Strategy() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold" data-testid="text-page-title">SOXL（bothside）, TSLL（longonly） TradingBot</h1>
        <p className="text-muted-foreground">Strategy Basket - Backtesting performance data</p>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <Card data-testid="card-chart-15min">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <div>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                15-Minute Strategy
              </CardTitle>
              <p className="text-sm text-muted-foreground mt-1">365 days backtesting</p>
            </div>
            <Badge variant="secondary">15min</Badge>
          </CardHeader>
          <CardContent>
            <div className="h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData15min} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="name" className="text-xs" />
                  <YAxis tickFormatter={(value) => `${value}%`} className="text-xs" />
                  <Tooltip
                    formatter={(value: number) => [`${value.toFixed(2)}%`, "Return"]}
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      border: "1px solid hsl(var(--border))",
                      borderRadius: "6px",
                    }}
                  />
                  <Legend />
                  <Bar dataKey="return" name="Return %" radius={[4, 4, 0, 0]}>
                    {chartData15min.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-4 grid grid-cols-2 gap-4">
              {backtestData.map((item) => (
                <div key={item.symbol} className="text-center p-3 rounded-md bg-muted/50">
                  <div className="text-sm font-medium text-muted-foreground">{item.symbol}</div>
                  <div className="text-2xl font-bold font-mono text-green-600 dark:text-green-400">
                    +{item.strategies[0].return}%
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card data-testid="card-chart-5min">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <div>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                5-Minute Strategy
              </CardTitle>
              <p className="text-sm text-muted-foreground mt-1">150 days backtesting</p>
            </div>
            <Badge variant="secondary">5min</Badge>
          </CardHeader>
          <CardContent>
            <div className="h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData5min} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="name" className="text-xs" />
                  <YAxis tickFormatter={(value) => `${value}%`} className="text-xs" />
                  <Tooltip
                    formatter={(value: number) => [`${value.toFixed(2)}%`, "Return"]}
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      border: "1px solid hsl(var(--border))",
                      borderRadius: "6px",
                    }}
                  />
                  <Legend />
                  <Bar dataKey="return" name="Return %" radius={[4, 4, 0, 0]}>
                    {chartData5min.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-4 grid grid-cols-2 gap-4">
              {backtestData.map((item) => (
                <div key={item.symbol} className="text-center p-3 rounded-md bg-muted/50">
                  <div className="text-sm font-medium text-muted-foreground">{item.symbol}</div>
                  <div className="text-2xl font-bold font-mono text-green-600 dark:text-green-400">
                    +{item.strategies[1].return}%
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card data-testid="card-summary">
        <CardHeader>
          <CardTitle>Strategy Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-3 px-4 font-medium">Symbol</th>
                  <th className="text-left py-3 px-4 font-medium">Timeframe</th>
                  <th className="text-right py-3 px-4 font-medium">Period</th>
                  <th className="text-right py-3 px-4 font-medium">Return</th>
                </tr>
              </thead>
              <tbody>
                {backtestData.flatMap((item) =>
                  item.strategies.map((strategy, idx) => (
                    <tr key={`${item.symbol}-${idx}`} className="border-b last:border-0" data-testid={`row-strategy-${item.symbol}-${strategy.timeframe}`}>
                      <td className="py-3 px-4 font-mono font-medium">{item.symbol}</td>
                      <td className="py-3 px-4">
                        <Badge variant="outline">{strategy.timeframe}</Badge>
                      </td>
                      <td className="py-3 px-4 text-right text-muted-foreground">{strategy.days} days</td>
                      <td className="py-3 px-4 text-right font-mono font-bold text-green-600 dark:text-green-400">
                        +{strategy.return}%
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
