import { useQuery } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Wifi, WifiOff, Loader2 } from "lucide-react";

export function ConnectionStatus() {
  const { data, isLoading, isError } = useQuery<{ connected: boolean; account?: { status: string } }>({
    queryKey: ["/api/account/status"],
    refetchInterval: 30000,
  });

  if (isLoading) {
    return (
      <Badge variant="secondary" className="gap-1">
        <Loader2 className="h-3 w-3 animate-spin" />
        <span>Connecting...</span>
      </Badge>
    );
  }

  if (isError || !data?.connected) {
    return (
      <Badge variant="destructive" className="gap-1" data-testid="status-disconnected">
        <WifiOff className="h-3 w-3" />
        <span>Disconnected</span>
      </Badge>
    );
  }

  return (
    <Badge variant="secondary" className="gap-1 bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400" data-testid="status-connected">
      <Wifi className="h-3 w-3" />
      <span>Connected</span>
    </Badge>
  );
}
