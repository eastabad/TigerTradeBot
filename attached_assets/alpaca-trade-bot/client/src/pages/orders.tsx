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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  ListOrdered,
  X,
  RefreshCw,
  Clock,
  CheckCircle,
  XCircle,
  AlertCircle,
} from "lucide-react";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import type { AlpacaOrder } from "@shared/schema";

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

function formatDateTime(dateString: string | null | undefined): string {
  if (!dateString) return "-";
  return new Date(dateString).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
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

function OrderTypeBadge({ type, limitPrice, stopPrice }: { type: string; limitPrice?: string | null; stopPrice?: string | null }) {
  let display = type.toUpperCase();
  if (type === "limit" && limitPrice) {
    display = `Limit @ $${parseFloat(limitPrice).toFixed(2)}`;
  } else if (type === "stop" && stopPrice) {
    display = `Stop @ $${parseFloat(stopPrice).toFixed(2)}`;
  } else if (type === "stop_limit" && stopPrice && limitPrice) {
    display = `Stop Limit @ $${parseFloat(stopPrice).toFixed(2)}`;
  }
  return (
    <Badge variant="outline" className="font-mono text-xs">
      {display}
    </Badge>
  );
}

export default function Orders() {
  const { toast } = useToast();

  const { data: openOrders, isLoading: openOrdersLoading, refetch: refetchOpenOrders } = useQuery<AlpacaOrder[]>({
    queryKey: ["/api/orders/open"],
  });

  const { data: orderHistory, isLoading: orderHistoryLoading, refetch: refetchOrderHistory } = useQuery<AlpacaOrder[]>({
    queryKey: ["/api/orders/history"],
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
      queryClient.invalidateQueries({ queryKey: ["/api/orders"] });
      queryClient.invalidateQueries({ queryKey: ["/api/trades"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Cancel Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleRefresh = () => {
    refetchOpenOrders();
    refetchOrderHistory();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold" data-testid="text-page-title">Orders</h1>
          <p className="text-muted-foreground">View and manage your trading orders</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={handleRefresh}
          data-testid="button-refresh-orders"
        >
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      <Tabs defaultValue="open" className="space-y-4">
        <TabsList data-testid="tabs-orders">
          <TabsTrigger value="open" data-testid="tab-open-orders">
            Open Orders
            {openOrders && openOrders.length > 0 && (
              <Badge variant="secondary" className="ml-2">
                {openOrders.length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="history" data-testid="tab-order-history">
            Order History
          </TabsTrigger>
        </TabsList>

        <TabsContent value="open">
          <Card>
            <CardHeader>
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
              ) : openOrders && openOrders.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Side</TableHead>
                      <TableHead>Order Type</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead className="text-right">Filled Qty</TableHead>
                      <TableHead className="text-right">Avg Fill Price</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Submitted</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {openOrders.map((order) => (
                      <TableRow key={order.id} data-testid={`row-order-${order.id}`}>
                        <TableCell className="font-medium font-mono">{order.symbol}</TableCell>
                        <TableCell><SideBadge side={order.side} /></TableCell>
                        <TableCell><OrderTypeBadge type={order.type} limitPrice={order.limitPrice} stopPrice={order.stopPrice} /></TableCell>
                        <TableCell className="text-right font-mono">{order.qty}</TableCell>
                        <TableCell className="text-right font-mono">{order.filledQty || "0"}</TableCell>
                        <TableCell className="text-right font-mono">
                          {formatCurrency(order.filledAvgPrice)}
                        </TableCell>
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
                <div className="flex flex-col items-center justify-center py-12 text-center text-muted-foreground">
                  <ListOrdered className="h-12 w-12 mb-3 opacity-50" />
                  <p className="font-medium">No open orders</p>
                  <p className="text-sm">Active orders will appear here</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="history">
          <Card>
            <CardHeader>
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
              ) : orderHistory && orderHistory.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Side</TableHead>
                      <TableHead>Order Type</TableHead>
                      <TableHead className="text-right">Qty</TableHead>
                      <TableHead className="text-right">Filled Qty</TableHead>
                      <TableHead className="text-right">Avg Fill Price</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Submitted</TableHead>
                      <TableHead className="text-right">Filled At</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {orderHistory.map((order) => (
                      <TableRow key={order.id} data-testid={`row-order-history-${order.id}`}>
                        <TableCell className="font-medium font-mono">{order.symbol}</TableCell>
                        <TableCell><SideBadge side={order.side} /></TableCell>
                        <TableCell><OrderTypeBadge type={order.type} limitPrice={order.limitPrice} stopPrice={order.stopPrice} /></TableCell>
                        <TableCell className="text-right font-mono">{order.qty}</TableCell>
                        <TableCell className="text-right font-mono">{order.filledQty || "0"}</TableCell>
                        <TableCell className="text-right font-mono">
                          {order.filledAvgPrice ? formatCurrency(order.filledAvgPrice) : "-"}
                        </TableCell>
                        <TableCell><StatusBadge status={order.status} /></TableCell>
                        <TableCell className="text-right text-xs font-mono text-muted-foreground">
                          {formatDateTime(order.submittedAt)}
                        </TableCell>
                        <TableCell className="text-right text-xs font-mono text-muted-foreground">
                          {order.filledAt ? formatDateTime(order.filledAt) : "-"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="flex flex-col items-center justify-center py-12 text-center text-muted-foreground">
                  <Clock className="h-12 w-12 mb-3 opacity-50" />
                  <p className="font-medium">No order history</p>
                  <p className="text-sm">Your completed orders will appear here</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
