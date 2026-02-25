# Trading Dashboard Design Guidelines

## Design Approach
**System**: Bootstrap 5 Dark Theme with professional financial UI enhancements
**References**: Bloomberg Terminal data density + TradingView chart aesthetics + Robinhood's clean metrics display

## Core Design Principles
- Information hierarchy prioritizes real-time data and critical alerts
- Professional financial aesthetic with emphasis on readability
- Dark theme optimized for extended monitoring sessions
- Data-dense layouts without overwhelming users

## Typography System

**Font Stack**: 
- Primary: 'Inter', system-ui (clean, readable at small sizes)
- Monospace: 'Roboto Mono' (for prices, timestamps, numerical data)

**Scale**:
- Display numbers (prices): 28-36px, medium weight
- Section headers: 20px, semibold
- Data labels: 14px, regular
- Table/list data: 13px, regular
- Timestamps: 11px, regular, reduced opacity

**Treatment**: Use monospace exclusively for all numerical data, timestamps, and currency values. Upper case sparingly for critical labels only.

## Layout System

**Spacing Primitives**: Tailwind units of 2, 3, 4, 6, 8 (p-2, m-4, gap-6, etc.)

**Dashboard Structure**:
- Top navigation bar: Fixed, 64px height
- Sidebar navigation: 240px width, collapsible to 72px (icons only)
- Main content area: Fluid with max-width constraint
- Grid system: 12-column for responsive data cards

**Content Organization**:
- Primary viewport: Real-time metrics cards (3-4 columns on desktop)
- Secondary area: Charts and visualization (60% width)
- Tertiary area: Recent activity feed (40% width, right-aligned)
- Bottom section: Full-width data table

## Component Library

### Navigation
- **Top Bar**: Logo left, search center, user profile/notifications right, settings icon
- **Sidebar**: Icon + label nav items, active state with left accent border (3px), group sections with subtle dividers

### Data Display Cards
- **Metric Cards**: Title (12px uppercase), large value (32px mono), change indicator (+/-), micro-sparkline chart, 16px padding
- **Status Cards**: Automation status, connection health, last update timestamp
- **Layout**: Consistent card heights within rows, subtle borders (1px), slightly elevated on hover

### Charts & Visualizations
- **Primary Chart**: Candlestick/line chart, 400px min-height, toolbar with timeframe toggles
- **Mini Charts**: Sparklines in metric cards, 40px height
- **Volume Bars**: Bottom-aligned beneath price charts
- **Grid Lines**: Subtle, dashed, low opacity

### Tables
- **Trade History**: Striped rows, sticky header, sortable columns
- **Columns**: Timestamp, pair, type, amount, price, total, status
- **Row Height**: 48px for comfortable scanning
- **Pagination**: Bottom-aligned, shows entries count

### Forms & Controls
- **Configuration Panel**: Two-column form layout, grouped sections
- **Inputs**: Full-width, consistent height (44px), clear labels above
- **Toggles**: For enable/disable features, large hit targets
- **Action Buttons**: Primary (gradient green for buy), danger (red for sell/stop), secondary (outlined)

### Alerts & Notifications
- **Toast Notifications**: Top-right corner, auto-dismiss, trade execution confirmations
- **Alert Banner**: Top of dashboard for critical system messages
- **Status Indicators**: Dot badges (green: active, yellow: warning, red: error)

## Visual Hierarchy

**Emphasis Levels**:
1. Critical numbers: Largest size, high contrast, monospace
2. Change indicators: Color-coded (green up, red down), with arrows
3. Supporting data: Medium size, standard weight
4. Metadata: Smallest size, reduced opacity (60%)

**Density**: Comfortable data density - information-rich without cramping. 20px min spacing between distinct data groups.

## Interaction Patterns

**Hover States**: Subtle background lightening (5% white overlay), no dramatic effects
**Active Trades**: Highlight row with left accent border
**Refresh Indicators**: Pulsing dot for live data, skeleton loaders for initial load
**Empty States**: Icon + message center-aligned within component boundaries

## Responsive Behavior

- Desktop (>1200px): Full multi-column layout as described
- Tablet (768-1199px): 2-column metric cards, stacked chart/activity
- Mobile (<768px): Sidebar collapses to overlay, single column cards, horizontal scroll tables with sticky first column

## Accessibility

- Maintain 4.5:1 contrast for all data text
- Focus indicators: 2px outline with offset
- Keyboard navigation for all interactive elements
- Screen reader labels for icon-only buttons

---

**No Images Required** - This is a data-centric dashboard application where all visual interest comes from data visualization, charts, and information architecture rather than photography or illustrations.