import { useLocation, Link } from "wouter";
import { useQuery } from "@tanstack/react-query";
import {
  LayoutDashboard,
  Briefcase,
  Activity,
  LineChart,
  Wallet,
  Webhook,
  Radar,
} from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarHeader,
} from "@/components/ui/sidebar";
import { Skeleton } from "@/components/ui/skeleton";
import type { AlpacaAccount } from "@shared/schema";

interface AccountInfo {
  accountId: number;
  name: string;
  symbols: string[];
  account: AlpacaAccount;
}

const menuItems = [
  {
    title: "Overview",
    url: "/",
    icon: LayoutDashboard,
  },
  {
    title: "Positions",
    url: "/positions",
    icon: Briefcase,
  },
  {
    title: "Strategy Basket",
    url: "/strategy",
    icon: LineChart,
  },
  {
    title: "Webhook Signals",
    url: "/signals",
    icon: Webhook,
  },
  {
    title: "Stock Scanner",
    url: "/scanner",
    icon: Radar,
  },
];

export function AppSidebar() {
  const [location] = useLocation();

  const { data: accounts, isLoading: accountsLoading } = useQuery<AccountInfo[]>({
    queryKey: ["/api/accounts"],
  });

  return (
    <Sidebar>
      <SidebarHeader className="p-4 border-b border-sidebar-border">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary">
            <Activity className="h-5 w-5 text-primary-foreground" />
          </div>
          <span className="text-base font-semibold">Trading Bot</span>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Navigation</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {menuItems.map((item) => {
                const isActive = location === item.url || 
                  (item.url !== "/" && location.startsWith(item.url));
                return (
                  <SidebarMenuItem key={item.title}>
                    <SidebarMenuButton
                      asChild
                      isActive={isActive}
                      data-testid={`link-nav-${item.title.toLowerCase().replace(" ", "-")}`}
                    >
                      <Link href={item.url}>
                        <item.icon className="h-4 w-4" />
                        <span>{item.title}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel>Accounts</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {accountsLoading ? (
                [1, 2, 3].map((i) => (
                  <SidebarMenuItem key={i}>
                    <div className="px-2 py-1.5">
                      <Skeleton className="h-5 w-full" />
                    </div>
                  </SidebarMenuItem>
                ))
              ) : (
                accounts?.map((acc) => {
                  const isActive = location === `/account/${acc.accountId}`;
                  const shortName = acc.name
                    .replace("Strategy ", "")
                    .replace(" (SOXL/TSLL)", ": SOXL/TSLL")
                    .replace(" (High-Cap Momentum)", ": High-Cap")
                    .replace(" (MAG7 + Short-Term)", ": MAG7");
                  return (
                    <SidebarMenuItem key={acc.accountId}>
                      <SidebarMenuButton
                        asChild
                        isActive={isActive}
                        data-testid={`link-account-${acc.accountId}`}
                      >
                        <Link href={`/account/${acc.accountId}`}>
                          <Wallet className="h-4 w-4" />
                          <span>{shortName}</span>
                        </Link>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })
              )}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
