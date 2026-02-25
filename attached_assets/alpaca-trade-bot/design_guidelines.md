# Alpaca Paper Trading Bot - Design Guidelines

## Design Approach
**System-Based Approach**: Drawing from financial trading platforms (Interactive Brokers, TD Ameritrade, Robinhood) with emphasis on data clarity and operational efficiency. This is a utility-focused application where functionality and information hierarchy are paramount.

## Core Design Principles
1. **Data First**: Trading information must be immediately scannable
2. **Operational Clarity**: Critical actions (buy/sell/cancel) must be obvious and error-resistant
3. **Status Transparency**: Real-time account state and order status always visible
4. **Minimal Distraction**: No unnecessary animations or visual flourishes

---

## Typography System
- **Primary Font**: Inter (via Google Fonts) for all UI text
- **Monospace Font**: JetBrains Mono for numerical data, prices, timestamps
- **Hierarchy**:
  - Page titles: text-2xl font-semibold
  - Section headers: text-lg font-medium
  - Data labels: text-sm font-medium text-gray-600
  - Numeric values: text-base font-mono
  - Timestamps: text-xs font-mono

## Layout System
**Spacing Units**: Use Tailwind units of 2, 4, 6, and 8 for consistency
- Component padding: p-4 or p-6
- Section spacing: space-y-6
- Card gaps: gap-4
- Container max-width: max-w-7xl

**Grid Structure**: 
- Dashboard: 3-column grid (sidebar nav + main content + info panel)
- Responsive: Stack to single column on mobile

---

## Component Library

### Navigation
- **Left Sidebar** (w-64): Fixed navigation with sections for Dashboard, Orders, Positions, Account, Settings
- Highlight active route with subtle background treatment
- Icons from Heroicons (outline style)

### Dashboard Layout
1. **Account Summary Bar** (top): Buying power, portfolio value, P&L - horizontal card layout
2. **Active Orders Table**: Real-time order status with action buttons
3. **Position Grid**: Current holdings with performance metrics
4. **Recent Activity Feed**: Chronological trade log

### Data Tables
- Striped rows for better readability
- Fixed headers for scrollable content
- Monospace for all numerical columns
- Status indicators: Small badges (text-xs px-2 py-1 rounded)
- Action buttons: Icon-only or icon + text (sm size)

### Forms & Controls
- **Order Entry Panel**: Compact form with symbol input, quantity, order type selector
- Input fields: Consistent height (h-10), clear labels above
- Primary actions: Full-width buttons within forms
- Validation feedback: Inline below inputs

### Status Indicators
- Order status badges: Filled (pending), Open (active), Cancelled, Rejected
- Connection status: Small indicator in top-right (Connected/Disconnected)
- Real-time updates: Subtle pulse animation only for critical changes

### Cards
- Border-based separation (border border-gray-200)
- Consistent padding: p-6
- Header + content structure
- No shadows unless overlaying (modals)

---

## Information Architecture

### Primary Views
1. **Dashboard**: Overview of account health and active positions
2. **Orders**: Order history, active orders, order entry
3. **Positions**: Current holdings with detailed metrics
4. **Account**: Paper trading account details, configuration
5. **Settings**: API configuration, trading parameters

### Data Density
- High information density preferred - traders expect to see multiple metrics at once
- Use horizontal space efficiently with multi-column layouts
- Group related data visually (border or background treatment)

---

## Icons
**Library**: Heroicons (outline) via CDN
- Navigation: home, chart-bar, clipboard-list, wallet, cog
- Actions: plus, trash, refresh, x-mark
- Status: check-circle, x-circle, clock, exclamation-triangle

---

## Accessibility
- All interactive elements meet 44x44px touch targets
- ARIA labels for icon-only buttons
- Keyboard navigation support for forms and tables
- High contrast for critical numerical data

---

## Images
**No hero images required** - This is a functional application. Only use imagery for:
- Empty states (placeholder illustrations for no orders/positions)
- Logo/branding in navigation header

---

## Responsive Behavior
- Desktop (lg+): Full 3-column layout
- Tablet (md): Collapsible sidebar, 2-column main area
- Mobile (base): Single column stack, hamburger menu for navigation
- Tables: Horizontal scroll on mobile with fixed first column

---

## Performance Considerations
- Minimal animations (only for critical state changes)
- Efficient table rendering for large order histories
- Real-time updates via WebSocket without full re-renders
- Lazy loading for historical data views