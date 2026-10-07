# CustomerLake Brand Guidelines

**Version:** 2.3.0 — Updated 2026-10-07 by @designer (V48 Full Brand Sweep: v39.0.0 (5799 lines, 17 pages, 4514-line backend). APP-LTV-INTEGRITY reviewed — backend calibration PASS, frontend dual-ROI display PASS. V46 ghost-completion caught: V48-UX-FIX-1 filed (4 sub-fixes). V47-AMBER-CLEANUP closed (0 occurrences). 3 new diff-badges approved: CMO-156, DATA NOTE, PILOT HYPOTHESIS. 36 tables with 36 captions. 7/9 brand dimensions pass.)  
**Status:** BINDING — All CustomerLake apps, dashboards, and demos MUST conform  
**Scope:** Databricks App (`customerlake`), AI/BI dashboards, Genie spaces, demo materials  
**Tag:** `customerlake_project: customerlake`

---

## 1. Brand Identity

### Brand Positioning
CustomerLake is an **AI-Driven Customer Intelligence Platform** built on Databricks. The brand conveys:
- **Intelligence** — data-driven, AI-unified, cross-source insight
- **Trust** — enterprise-grade, identity-resolved, auditable
- **Clarity** — clean, information-dense, scannable interfaces
- **Differentiation** — what only CustomerLake can do vs. legacy CDPs

### Brand Voice
- Professional but accessible
- Data-first — metrics before narratives
- Confidence without hype
- Action-oriented copy (verbs over nouns)

### Logo Treatment
- **Wordmark:** "CustomerLake" — always one word, capital C and L
- **Gradient treatment:** `linear-gradient(135deg, #4F8FF7, #818CF8)` for the wordmark
- **Tagline:** "AI Customer Intelligence" — uppercase, 11px, letter-spacing 0.5px
- **Minimum clear space:** 16px on all sides
- **Footer attribution:** "Powered by Databricks" + "cdm_tmforum · Lakebase · Unity Catalog"

---

## 2. Color Palette

### Primary Palette
| Token | Hex | Usage |
|---|---|---|
| `--accent` | `#4F8FF7` | Primary action, links, active states, entity references |
| `--accent-hover` | `#3A7DE8` | Hover state for primary actions |
| `--accent-light` | `rgba(79,143,247,0.12)` | Active nav background, subtle highlights |
| `--info` | `#818CF8` | Secondary accent, gradient endpoint, info badges |

### Semantic Palette
| Token | Hex | Usage |
|---|---|---|
| `--success` | `#34D399` | Positive states, revenue, approved, good standing |
| `--success-bg` | `rgba(52,211,153,0.12)` | Success badge backgrounds |
| `--warning` | `#FBBF24` | Caution states, dunning, credit hold, Oracle ERP source |
| `--warning-bg` | `rgba(251,191,36,0.12)` | Warning badge backgrounds |
| `--danger` | `#F87171` | Negative states, fraud, rejected, collections |
| `--danger-bg` | `rgba(248,113,113,0.12)` | Danger badge backgrounds |

### Surface Palette (Dark Theme)
| Token | Hex | Usage |
|---|---|---|
| `--bg-primary` | `#0F1117` | Page background |
| `--bg-secondary` | `#1A1D29` | Sidebar, inputs, detail panels |
| `--bg-card` | `#1E2130` | Card backgrounds |
| `--bg-hover` | `#252840` | Hover states, neutral badge bg, bar chart tracks |
| `--border` | `#2D3148` | All borders, dividers |

### Text Palette
| Token | Hex | Usage |
|---|---|---|
| `--text-primary` | `#E8EAF0` | Primary content, headings, data values |
| `--text-secondary` | `#9498B0` | Secondary content, table cell text, descriptions |
| `--text-muted` | `#8B8FA8` | Labels, captions, timestamps, KPI labels |

### Source System Colors
| Source | Background | Text Color | Usage |
|---|---|---|---|
| TMF_PARTY | `rgba(79,143,247,0.2)` | `--accent` | TMF source badges |
| SALESFORCE | `rgba(52,211,153,0.2)` | `--success` | Salesforce source badges |
| ORACLE_ERP | `rgba(251,191,36,0.2)` | `--warning` | Oracle ERP source badges |

### Channel Data-Viz Colors (v1.1)
When coloring by channel (not by semantic meaning), use these non-semantic assignments:
| Channel | Color | Rationale |
|---|---|---|
| email | `--accent` (#4F8FF7) | Primary digital channel |
| paid_social | `--info` (#818CF8) | Secondary digital channel |
| sms | `#6B8CC7` | Muted blue — distinct but not semantic |
| push_notification | `#9498B0` | Neutral — text-secondary value |
| direct_mail | `#6B6F88` | Neutral — text-muted value |

### Color Rules
1. **WCAG 2.1 AA compliance is MANDATORY** — all text/background combinations must meet 4.5:1 contrast ratio for normal text, 3:1 for large text
2. Never use color alone to convey meaning — always pair with text labels, icons, or patterns
3. Semantic colors are reserved — do NOT use `--success` for non-positive states or `--warning` for non-caution states
4. Source system colors are locked — new sources follow the same `rgba(color, 0.2)` pattern
5. Channel differentiation MUST use the Channel Data-Viz palette above, NOT semantic colors (v1.1)

---

## 3. Typography

### Font Stack
```css
font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
```
**Rationale:** System font stack ensures fast rendering, platform-native feel, and no FOUT.

### Type Scale
| Element | Size | Weight | Line Height | Tracking | Usage |
|---|---|---|---|---|---|
| Page Title (h2) | 22px | 600 | 1.3 | Normal | Page header titles |
| Logo (h1) | 18px | 700 | 1.3 | Normal | Sidebar wordmark |
| Card Title | 15px | 600 | 1.5 | Normal | Section headings within cards |
| Body | 14px | 400 | 1.5 | Normal | Default text, inputs, nav items |
| Table Body | 13px | 400 | 1.5 | Normal | Table cells, list items |
| Table Header | 11px | 500 | 1.5 | 0.5px | Column headings (uppercase) |
| KPI Value | 28px | 700 | 1.2 | Normal | Dashboard metric values |
| KPI Label | 12px | 400 | 1.5 | 0.5px | Metric labels (uppercase) |
| KPI Sub | 12px | 400 | 1.5 | Normal | Metric supplementary text |
| Badge | 11px | 500 | 1.0 | Normal | Status badges |
| Source Badge | 10px | 600 | 1.0 | Normal | Source system tags |
| Caption | 11px | 400 | 1.5 | 0.5px | Footer text, subtitles |
| Hero KPI Value | 32–36px | 700 | 1.2 | Normal | Page north-star metrics (CFO Bottom Line, True Causal ROI) |
| Capability Hero Count | 48px | 700 | 1.2 | Normal | Single dramatic count on ComparisonPage capability-gap hero |
| Diff Badge | 10px | 600 | 1.0 | Normal | UNIQUE/DEDUP feature callouts |

### Typography Rules
1. **Maximum 3 font sizes on any single view** (KPI value + body + label)
2. Uppercase is reserved for labels, table headers, and badges — never for body text
3. Letter-spacing (`0.5px`) accompanies uppercase treatments only
4. Bold (`700`) is reserved for KPI values, the wordmark, and source badges
5. Semi-bold (`600`) for headings only — do not use for emphasis in body text

---

## 4. Iconography

### Current State: RESOLVED (v1.1)
Emoji nav icons replaced with Lucide SVG Icon component (UX-FIX-1, completed 2026-09-29). 11 SVG paths, 18px default, 1.5px stroke, currentColor, aria-hidden.

### Required Standard
- **Icon system:** Use inline SVG icons or a lightweight icon font (Lucide, Phosphor, or similar)
- **Icon size:** 18px (nav), 16px (inline), 24px (empty states)
- **Icon color:** Inherits from parent text color (uses `currentColor`)
- **Icon weight:** 1.5px stroke for line icons
- **Nav icons:** Fixed 24px width, center-aligned

### Recommended Icon Mapping
| Page | Current (Emoji) | Required (SVG/Icon) |
|---|---|---|
| Overview | ⌂ | `home` or `layout-dashboard` |
| Profile Search | 👤 | `user-search` or `users` |
| Audience Builder | 🎯 | `target` or `crosshair` |
| Campaigns | 📊 | `bar-chart-3` or `trending-up` |
| Identity Steward | 🔗 | `git-merge` or `link-2` |

### Icon Rules
1. **NEVER use emoji in production UI** — rendering varies across OS/browser
2. All icons must be single-color, inheriting `currentColor`
3. Decorative icons get `aria-hidden="true"`; meaningful icons get `aria-label`
4. Icon-only buttons require `aria-label` and visible tooltip

---

## 5. Layout & Spacing

### 8px Base Grid
All spacing values MUST be multiples of 8px:

| Token | Value | Usage |
|---|---|---|
| `--space-1` | 4px | Tight internal padding (badges, inline gaps) |
| `--space-2` | 8px | Minimum gap, icon margins, bar chart row spacing |
| `--space-3` | 12px | Card internal margins, search bar gap |
| `--space-4` | 16px | Card padding, grid gap, KPI card padding |
| `--space-5` | 20px | Sidebar padding, card body padding |
| `--space-6` | 24px | Main content padding, page header margin |
| `--space-8` | 32px | Main content horizontal padding |
| `--space-10` | 40px | Loading/empty state padding |

**Exception:** 4px is allowed for tight badge internal padding and 2px for badge vertical padding.

### Layout Structure
```
┌─────────────────────────────────────────────────┐
│ App Layout (flex, min-height: 100vh)             │
│ ┌──────────┬───────────────────────────────────┐ │
│ │ Sidebar  │ Main Content                      │ │
│ │ 240px    │ flex: 1, padding: 24px 32px       │ │
│ │          │ max-height: 100vh, overflow-y      │ │
│ │ Logo     │ ┌─ Page Header ─────────────────┐ │ │
│ │ Nav      │ │ h2 + description              │ │ │
│ │ ...      │ ├─ KPI Grid ────────────────────┤ │ │
│ │ Footer   │ │ auto-fit, minmax(200px, 1fr)  │ │ │
│ │          │ ├─ Content Grid (.grid-2) ──────┤ │ │
│ │          │ │ 1fr 1fr                        │ │ │
│ │          │ ├─ Data Table ──────────────────┤ │ │
│ │          │ └───────────────────────────────┘ │ │
│ └──────────┴───────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Border Radius
| Token | Value | Usage |
|---|---|---|
| `--radius` | 8px | Buttons, inputs, badges (large), small cards |
| `--radius-lg` | 12px | Cards, KPI cards |
| Badge radius | 10px | Status badges |
| Source badge | 4px | Source system tags |
| Confidence bar | 3px | Progress bars |

### Shadow
| Token | Value | Usage |
|---|---|---|
| `--shadow` | `0 2px 8px rgba(0,0,0,0.3)` | General card elevation (reserved, sparingly used) |
| Detail panel | `-4px 0 24px rgba(0,0,0,0.4)` | Slide-out panels |

---

## 6. Component Standards

### KPI Cards
- Layout: Grid, `auto-fit, minmax(200px, 1fr)`, gap 16px
- Structure: Label (uppercase, muted) → Value (28px, bold) → Sub-text (12px, secondary)
- Colored values allowed for semantic emphasis (revenue=success, accent for highlighted)
- Background: `--bg-card` with `--border` outline

### Data Tables
- Full width, collapse borders
- Header: 11px, uppercase, 500 weight, muted color, 0.5px tracking
- Body: 13px, secondary text color
- Row hover: `--bg-hover` background
- Cell padding: 10px vertical, 12px horizontal (body); 8px/12px (header)
- Entity IDs: Use `.entity-link` style (accent color, pointer cursor, 500 weight)

### Badges
- Inline-block, 2px vertical / 8px horizontal padding, 10px radius
- Semantic coloring: success/warning/danger/info/neutral
- 11px font, 500 weight
- Source badges: 1px/6px padding, 4px radius, 10px font, 600 weight

### Buttons
- Base: 6px/14px padding, `--radius` border-radius, 13px font, 500 weight
- Primary: `--accent` bg, white text
- Ghost: transparent bg, `--border` outline, secondary text
- Small (`.btn-sm`): 4px/10px padding, 12px font
- Success/Danger: semantic bg, black text

### Search Bar
- Flex row, 8px gap
- Input: flex: 1, `--bg-secondary` background, `--border` outline
- Focus: `--accent` border color
- Select: min-width 140px
- 8px/12px padding, 14px font, `--radius` border-radius

### Cards
- Background: `--bg-card`, border: 1px `--border`, radius: `--radius-lg`
- Padding: 20px
- Margin-bottom: 16px
- Title: 15px, 600 weight, margin-bottom 12px

### Detail Panel (Slide-out)
- Fixed position, right: 0, width: 520px, full height
- Background: `--bg-secondary`
- Left border: 1px `--border`
- Shadow: `-4px 0 24px rgba(0,0,0,0.4)`
- Padding: 24px
- Z-index: 100
- Close button: absolute, top 16px, right 16px

### Confidence Bars
- Track: 60px wide, 6px height, `--bg-hover`, 3px radius
- Fill: 3px radius, semantic color based on value
  - >= 80%: `--success`
  - >= 50%: `--warning`
  - < 50%: `--danger`

### Bar Charts (Horizontal)
- Row: flex, center-aligned, 8px gap, 6px margin-bottom
- Label: 100px width, 12px, right-aligned, secondary text
- Track: flex: 1, 20px height, `--bg-hover`, 4px radius
- Fill: Height 100%, 4px radius, 11px white text, min-width 30px
- **Accessibility (v1.1):** Each bar-chart-row MUST have `role="img"` and `aria-label="{label}: {value}"` for screen readers

### Differentiator Badges
- `.diff-badge`: 2px/6px padding, 4px radius, 10px, 600 weight
- Color: `rgba(79,143,247,0.15)` bg, `--accent` text
- Used for UNIQUE, DEDUP, RESOLVE, MULTI-CH, CONSENT, ATTRIBUTED, ENTITY-LEVEL, CTR, ROI, OMNICHANNEL, COST + ROI, OBSERVE, ALIGN, CHURN, PROTECT, DISPUTE, CHANNEL, COST/SAVE, DELIVERY, ENABLE, ENTITY, LOOP, OPTIMIZE, SAVE RATE, TRANSPARENT, vs ACQUIRE, HOLDOUT, TRUE ROI, RISK-TIER, SUPPRESS, CAUSAL, P-VALUE, CONFIDENCE-INTERVAL, STATISTICAL-POWER, CFO-READY, COMPETITOR COMPARISON, CONFLICT-AWARE, COST-PER-ENTITY, COUNTERFACTUAL, CROSS-SOURCE, IDENTITY-RESOLVED, LIVE DATA, LTV, ML-CHANNEL-WASTE, REVENUE-LINKED, TCO-TRANSPARENT, ZERO MARKETING CLAIMS, IN-PERIOD, ML-PROJECTED, EVERY NUMBER QUERYABLE, CAPABILITIES FIRST, TERMINOLOGY, CALIBRATED, NEEDS RECALIBRATION, CMO-156, DATA NOTE, PILOT HYPOTHESIS
- **Pill badges** (capability feature tags): `var(--success-bg)` bg, `var(--success)` text, `4px 12px` padding, `borderRadius: 16px` (full pill), 12px, 500 weight

---

## 7. Responsive Design

### Breakpoints
| Breakpoint | Width | Behavior |
|---|---|---|
| Desktop | > 768px | Full layout: sidebar + main content |
| Mobile | <= 768px | Sidebar hidden, main padding reduced, grids collapse |

### Mobile Adaptations
```css
@media (max-width: 768px) {
  .sidebar { display: none; }
  .main-content { padding: 16px; }
  .kpi-grid { grid-template-columns: 1fr 1fr; }
  .grid-2 { grid-template-columns: 1fr; }
  .detail-panel { width: 100%; }
}
```

### Responsive Rules
1. KPI grid: `auto-fit, minmax(200px, 1fr)` — collapses gracefully
2. Content grids: 2-column on desktop, 1-column on mobile
3. Detail panel: Full width on mobile (520px on desktop)
4. Tables: Horizontal scroll wrappers applied to all 10+ tables (UX-FIX-5 RESOLVED)
5. ~~No hamburger menu exists~~ RESOLVED (UX-FIX-2): Mobile hamburger menu, sidebar overlay, close button, Escape handler

---

## 8. Accessibility (WCAG 2.1 AA)

### Mandatory Requirements
1. **Color contrast:** All text meets 4.5:1 (normal) or 3:1 (large) against its background
2. **Focus indicators:** All interactive elements must have visible focus outlines
3. **Keyboard navigation:** Full app navigable via Tab/Shift-Tab/Enter/Escape
4. **ARIA labels:** All icon buttons, close buttons, and non-text controls
5. **Semantic HTML:** Proper heading hierarchy, landmark regions, `<nav>`, `<main>`, `<table>` with `<caption>`
6. **Screen reader:** Loading/empty states announced via `aria-live="polite"`
7. **Motion:** Respect `prefers-reduced-motion` for transitions — add `@media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation-duration: 0.01ms !important; transition-duration: 0.01ms !important; } }` (A16, not yet implemented)
8. **Text resize:** Layout must not break at 200% zoom

### Current Compliance Issues (REMEDIATION REQUIRED)

| # | Issue | Severity | WCAG Criterion |
|---|---|---|---|
| A1 | No `<nav>` landmark wrapping sidebar navigation | ~~High~~ RESOLVED | 1.3.1 Info and Relationships |
| A2 | No `<main>` landmark wrapping content area | ~~High~~ RESOLVED | 1.3.1 Info and Relationships |
| A3 | Nav items are `<div>` not `<button>` or `<a>` — not keyboard focusable | ~~Critical~~ RESOLVED | 2.1.1 Keyboard |
| A4 | No visible focus indicators on nav items, table rows, entity links | ~~Critical~~ RESOLVED | 2.4.7 Focus Visible |
| A5 | Close button (✕) lacks `aria-label="Close detail panel"` | ~~High~~ RESOLVED | 4.1.2 Name, Role, Value |
| A6 | Tables lack `<caption>` elements | ~~Medium~~ RESOLVED | 1.3.1 Info and Relationships |
| A7 | Loading states not announced via `aria-live` | ~~Medium~~ RESOLVED | 4.1.3 Status Messages |
| A8 | Emoji nav icons inconsistent across platforms, no `aria-hidden` | ~~Medium~~ RESOLVED | 1.1.1 Non-text Content |
| A9 | Detail panel has no focus trap — Tab can escape to covered content | ~~High~~ RESOLVED | 2.4.3 Focus Order |
| A10 | Mobile: no navigation mechanism when sidebar is `display:none` | ~~Critical~~ RESOLVED | 2.4.5 Multiple Ways |
| A11 | Status badge colors alone convey meaning — need text redundancy | ~~Medium~~ RESOLVED | 1.4.1 Use of Color |
| A12 | Bar chart rows lack role="img" and aria-label for screen readers | ~~Medium~~ RESOLVED | 1.1.1 Non-text Content |
| A13 | Tab switchers lack role="tablist"/"tab"/"tabpanel" and aria-selected | ~~Medium~~ RESOLVED | 4.1.2 Name, Role, Value |
| A14 | Optimization recommendation cards lack role="region" and aria-label | ~~Medium~~ RESOLVED | 1.3.1 Info and Relationships |
| A15 | Methodology transparency banner lacks role="note" | ~~Low~~ RESOLVED | 1.3.1 Info and Relationships |
| A16 | No `prefers-reduced-motion` media query for CSS transitions | ~~Low~~ RESOLVED | 2.3.3 Animation from Interactions |

---

## 9. UX Audit Findings

### Strengths
- Clean, professional dark theme with good information density
- Consistent use of CSS custom properties — easy to theme
- KPI cards effectively surface key metrics at a glance
- Source system color coding is distinctive and scannable
- Differentiator badges (UNIQUE, DEDUP, etc.) effectively communicate CustomerLake value
- Detail panel slide-out is a good pattern for drill-down without losing context
- Good separation of concerns: 5 focused pages, each with clear purpose

### Issues Requiring Remediation

| # | Finding | Priority | Assigned To |
|---|---|---|---|
| UX-1 | **Emoji navigation icons** — inconsistent rendering across OS/browser. Replace with SVG icon system (Lucide recommended). | High | @app-developer |
| UX-2 | **No mobile navigation** — sidebar is `display:none` on mobile with no hamburger menu or alternative nav. Users on mobile are stranded. | Critical | @app-developer |
| UX-3 | **No keyboard navigation** — nav items are plain `<div>` elements, not focusable. Must be `<button>` or `role="button"` with `tabindex="0"` and keyboard handlers. | Critical | @app-developer |
| UX-4 | **Missing semantic landmarks** — no `<nav>`, `<main>`, `<section>` elements. Screen readers cannot navigate structure. | High | @app-developer |
| UX-5 | **No focus indicators** — CSS has no `:focus-visible` styles. Add `outline: 2px solid var(--accent); outline-offset: 2px` for all interactive elements. | High | @app-developer |
| UX-6 | **Detail panel has no focus trap** — when panel opens, focus should move to panel and Tab should cycle within it. Escape should close. | High | @app-developer |
| UX-7 | **Tables not horizontally scrollable** — on narrow viewports, tables overflow. Wrap in `overflow-x: auto` container. | Medium | @app-developer |
| UX-8 | **No loading skeletons** — plain text "Loading..." is jarring. Consider skeleton screens for KPI cards and tables. | Low | @app-developer |
| UX-9 | **No error state UI** — API failures show generic HTTPException text. Need user-friendly error cards with retry actions. | Medium | @app-developer |
| UX-10 | **No empty state illustration** — "No items" message is plain text. Add an icon and constructive message. | Low | @app-developer |

---

## 10. Dashboard & Genie Space Standards

When creating AI/BI dashboards or Genie spaces tagged `customerlake_project: customerlake`:

1. **Color theme:** Use dark theme matching the app palette where possible
2. **KPI naming:** Prefix with context (e.g., "Resolved Entities" not "Total Count")
3. **Chart colors:** Use the semantic palette — success for positive trends, danger for alerts
4. **Titles:** Follow brand voice — professional, data-first, action-oriented
5. **Descriptions:** Every dashboard widget must have a 1-line description explaining the metric
6. **Differentiator callout:** At least one widget per dashboard must highlight what CustomerLake does that legacy CDPs cannot

---

## 11. Implementation Checklist for @app-developer

### Critical — ALL RESOLVED (UX-FIX-1 through UX-FIX-11)
- [x] Replace emoji nav icons with SVG/Lucide icons (UX-FIX-1)
- [x] Add `<nav>`, `<main>` semantic landmarks (UX-FIX-4)
- [x] Make nav items keyboard-focusable (UX-FIX-3)
- [x] Add visible `:focus-visible` outlines (UX-FIX-3)
- [x] Add mobile hamburger menu (UX-FIX-2)

### High Priority — ALL RESOLVED
- [x] Add `aria-label` to close buttons (UX-FIX-4)
- [x] Implement focus trap for detail panel (UX-FIX-4)
- [x] Add `<caption>` to all data tables (UX-FIX-5)
- [x] Add `aria-live="polite"` to loading/empty states (UX-FIX-5)

### Medium Priority — ALL RESOLVED + NEW
- [x] Wrap tables in scrollable container (UX-FIX-5)
- [x] Add user-friendly error state UI with retry (UX-FIX-5)
- [x] Ensure all badge meanings have text redundancy (UX-FIX-6)
- [x] Remove dead emoji statusIcon() function (V4-UX-FIX-1)
- [x] Fix semantic color misuse in channel bar charts (V4-UX-FIX-2)
- [x] Add ARIA labels to all bar chart rows (V4-UX-FIX-3)

### Low Priority (Polish)
- [x] Fix churn_risk_score display (UX-FIX-10)
- [x] Align confidence bar thresholds (UX-FIX-11)
- [x] Update footer version (V4-UX-FIX-4 — now needs V7-UX-FIX-1 to bump to v6.0)
- [ ] Loading skeleton screens for KPI cards
- [ ] Empty state illustrations with constructive messaging
- [x] `prefers-reduced-motion` media query for transitions (V6-UX-FIX-BUNDLE)

### V10 Audit Findings (2026-09-30) — Polish Only
- [x] **V10-UX-FIX-1:** Footer version stale at `v7.0`. PARTIALLY FIXED to v8.0 — superseded by V11-UX-FIX-3.
- [ ] **V10-UX-FIX-2:** Profile detail panel campaign attributions (line 715) render all purposes with `badge-success`. Should use `badge-info` or map purpose to semantic color.
- [ ] **V10-UX-FIX-3:** Steward error dismiss button (line 1268) uses Unicode `✕` text instead of `<Icon d={icons.x} />` SVG. Consistency fix — use the Icon component.

### V35 Audit Findings (2026-10-05) — V34-Fix Verification + Full Brand Sweep
- [x] **V34-UX-FIX-1:** VERIFIED APPLIED. L843 and L920 banner padding changed from 10px 14px to 12px 16px. ON 8px grid.
- [x] **V34-UX-FIX-2:** VERIFIED APPLIED. L671: UNIQUE. L705: TRANSPARENT. L846: UNIQUE. L865: UNIQUE. All approved.
- [x] **V35-UX-FIX-1 (Low):** RESOLVED — V37 verified: 126 diff-badges audited, all on approved list. No unapproved labels remain.
- [x] **V35-UX-FIX-2 (Low):** RESOLVED — V37 verified: L980 shows padding 12px 16px. Both on 8px grid.

### V37 Audit Findings (2026-10-05) — DEMO-WALKTHROUGH-2 (v29.0.0) + Full Brand Sweep

**Scope:** 5257-line app (index.html, 16 pages) + app.py backend (v28.0.0 declared). Delta: +348 lines from V36 (Demo Walkthrough page + Guided Demo Mode).

**PASS — 7/9 Brand Dimensions:**
- Color palette: 5 demo steps use semantic colors correctly (IDENTIFY=accent, ACTIVATE=success, MEASURE=warning, OPTIMIZE=danger, DIFFERENTIATE=info)
- Typography: Hero 28px/700 gradient, card titles 15px/600, metric values 18px/700, labels 10px/uppercase/0.5px
- Iconography: icons.megaphone (L599) is Lucide SVG, aria-hidden via Icon component
- Layout: Card 24px padding, 16px gap/margin, hero 40/24/32px — all 8px multiples
- Components: bg-card, border, radius-lg — match spec
- Responsive: L562-566 breakpoint collapses cards, overflow-x on step bar
- DemoStepBar a11y: `<button>` steps with aria-current=step, Exit has aria-label

**FAIL — 3 issues filed to @app-developer:**
- [x] **V37-UX-FIX-1 (Medium, WCAG 2.1.1):** VERIFIED V38. L5157: tabIndex={0}, role="button", aria-label, onKeyDown with Enter/Space. PASS.
- [x] **V37-UX-FIX-2 (Medium, Version Sync):** VERIFIED V38. app.py L126=29.0.0, L3776=29.0.0, footer L5318=v29.0.0. All three in sync. PASS.
- [x] **V37-UX-FIX-3 (Low, 8px Grid):** VERIFIED V38. L533 now `4px 12px`. On grid. PASS.

### V38 Audit Findings (2026-10-05) — V37-Fix Verification + APP-DUAL-ROI + Full Brand Sweep

**Scope:** 5335-line app (index.html, 16 pages) + app.py backend (v29.0.0, 3776 lines, 36+ endpoints). Delta: +78 lines from V37 (APP-DUAL-ROI dual narrative, V37-fix-1/2/3).

**V37-FIX VERIFICATION (3/3 PASS):**
1. V37-UX-FIX-1 (keyboard a11y): L5157 — tabIndex, role="button", aria-label, onKeyDown. PASS.
2. V37-UX-FIX-2 (version sync): app.py L126/L3776 + footer L5318 all 29.0.0. PASS.
3. V37-UX-FIX-3 (8px grid): L533 padding `4px 12px`. PASS.

**APP-DUAL-ROI REVIEW (L775-875) — 9/9 BRAND DIMENSIONS PASS:**
- Color palette: Campaign ROI=accent-themed (muted), LTV ROI=success-themed. Semantic usage correct.
- Typography: 32px/700 hero KPIs, 12px labels, 10px methodology. Match spec.
- Iconography: icons.barChart + icons.trendingUp via Lucide SVG Icon component.
- Layout: 4-column hero grid, 12px/16px padding (on grid), 16px margins.
- Components: KPI cards with left accent borders, table with caption, role="note" banner.
- Responsive: Inherits existing grid collapse patterns.
- Accessibility: role="note" on explanation banner, caption on channel table, text+color badges.
- Contrast: Campaign hero 4.7:1 (#9498b0 on accent-light), LTV hero 6.6:1 (#34d399 on success-bg) — both PASS large-text 3:1.
- Diff-badges: IN-PERIOD, ML-PROJECTED, UNIQUE, IDENTITY-RESOLVED, ML-CHANNEL-WASTE, CROSS-SOURCE — all approved.

**NEW APPROVED DIFF-BADGES (added to §6):** IN-PERIOD, ML-PROJECTED, EVERY NUMBER QUERYABLE (ComparisonPage L4552).

**ZERO NEW ISSUES FILED.** App is brand-clean at v29.0.0.

### V11 Audit Findings (2026-09-30) — Incrementality Dashboard
- [x] **V11-UX-FIX-1 (Low):** RESOLVED — Added `role="note"` to Incrementality methodology div (line 2482). WCAG 1.3.1 compliant.
- [x] **V11-UX-FIX-2 (Medium):** RESOLVED — Added `role="img"` + `aria-label` to both bars. Added naive ROI comparison bar (muted, 60% opacity) above true incremental bar with HOLDOUT diff-badge. WCAG 1.1.1 compliant.
- [x] **V11-UX-FIX-3 (Low):** RESOLVED — Footer updated from v8.0 to v9.0 (line 2569). Supersedes V10-UX-FIX-1.

### V12 Audit Findings (2026-09-30) — Dedicated Incrementality Review (UX-REVIEW-INCR-DASH)
- [x] **V12-UX-FIX-1 (Medium):** RESOLVED — Purpose table rows (L2384) now have `tabIndex={0}`, `role="button"`, and `onKeyDown` with Enter/Space handlers. WCAG 2.1.1 compliant. Verified V13.
- [x] **V12-UX-FIX-2 (Medium):** RESOLVED — Risk-tier lift label (L2359) now uses conditional sign pattern. No more "+-N%" bug. Verified V13.
- [ ] **V12-UX-FIX-3 (Low):** Naive vs True ROI comparison still uses global naive ROI. Acceptable for demo scope.
- [x] **V12-UX-FIX-4 (Low):** RESOLVED — FastAPI metadata version updated to "9.0.0" (app.py L121), matching footer v9.0. Verified V13.

### V13 Audit Findings (2026-09-30) — CustomerLake Executive Overview Dashboard Audit (UX-REVIEW-V13)

**Scope:** 2-page dashboard (18 widgets: 6 counters, 12 bar charts), 12 datasets from `cdm_tmforum._metrics.customerlake_*`.

**PASS:**
- Widget titles are professional, data-first, and contextual
- All datasets reference proper customerlake_* metric views
- Chart encodings use appropriate categorical groupings
- KPI counters surface the right headline metrics

**FAIL — 3 issues filed to @data-analyst:**
- [x] **DASH-UX-FIX-1 (Medium):** VERIFIED V18 — All 20 widgets now have 1-line descriptions. Per §10 rule 5: "Every dashboard widget must have a 1-line description explaining the metric." Each should explain what it measures and why it matters.
- [x] **DASH-UX-FIX-2 (Medium):** VERIFIED V18 — Both pages now have differentiator text widgets ("Why CustomerLake?" + "CustomerLake Activation Edge"). Per §10 rule 6: "At least one widget per dashboard must highlight what CustomerLake does that legacy CDPs cannot." Supports CEO directive #5.
- [ ] **DASH-UX-FIX-3 (Low):** Bar chart axis labels show raw column names (destination_system, source_instance, etc.) instead of human-readable labels.

### V14 Audit Findings (2026-10-01) — Full Brand Compliance Sweep (UX-REVIEW-V14)

**Scope:** 2762-line app (index.html, 9 pages) + app.py backend + Executive Dashboard status. Version: footer v11.0, app.py 10.0.0.

**PASS — All Critical/High/Medium Holding:**
- All 22 tables have `<caption>` elements
- All 9 loading states have `role="status"` + `aria-live="polite"`
- All 13 bar-chart-rows have `role="img"` + `aria-label`
- No native `alert()` calls remaining (V9-UX-FIX-2 verified)
- V12-UX-FIX-1 (purpose row keyboard a11y) HOLDING
- V12-UX-FIX-2 (risk-tier lift sign bug) HOLDING
- V11-UX-FIX-1/2/3 HOLDING
- All semantic landmarks (`<nav>`, `<main>`) in place
- Mobile hamburger navigation with Escape handler
- Focus-visible outlines on all interactive elements
- Focus trap on detail panels
- `prefers-reduced-motion` media query
- Color palette, typography, iconography all match brand guidelines
- Differentiator cards on Overview, ML Predictions, and Incrementality pages

**FAIL — 1 low issue:**
- [ ] **V14-UX-FIX-1 (Low):** Footer version `v11.0` (L2749) doesn't match app.py FastAPI version `10.0.0` (L121). Keep in sync per brand consistency. Update app.py to `"11.0.0"`.

**KNOWN DEFERRED (Low — acceptable for demo):**
- V10-UX-FIX-2: L724 attribution purposes all use `badge-success`
- V10-UX-FIX-3: L1352 steward dismiss `✕` Unicode instead of SVG Icon
- V12-UX-FIX-3: Naive vs True ROI uses global naive

**DASHBOARD STATUS:** DASH-UX-FIX-1/2/3 in_progress with @data-analyst. Pending verification in next review.

### V16 Audit Findings (2026-10-01) — Identity Revenue Impact + Full Brand Sweep (UX-REVIEW-V16)

**Scope:** 2762-line app (index.html, 9 pages) + app.py backend (v11.0.0). Covers APP-IDENTITY-REV (new Steward Revenue Impact section) and V14-UX-FIX-1 verification.

**V14-UX-FIX-1 VERIFIED:** Footer v11.0 (L2749) matches app.py 11.0.0 (L121). PASS.

**APP-IDENTITY-REV (L1374-1461) — 9/9 checks PASS:**
- Loading state: `role="status"` + `aria-live="polite"` (WCAG 4.1.3)
- Differentiator banner: `role="note"` (WCAG 1.3.1)
- Color semantics: success/danger/warning correctly applied
- Bar chart: `role="img"` + `aria-label` (WCAG 1.1.1)
- Table: `<caption>`, `<thead>`, `.table-scroll`, proper badge semantics
- Typography, layout, backend security all compliant

**FAIL — 3 low issues filed to @app-developer:**
- [x] **V16-UX-FIX-1 (Low):** RESOLVED — L1398 now shows `minmax(200px, 1fr)`. Verified V17.
- [x] **V16-UX-FIX-2 (Low):** RESOLVED — L1379-1384 shows `error-card` + `role="alert"` + retry. Verified V17.
- [x] **V16-UX-FIX-3 (Low — enhancement):** RESOLVED — L1421-1427 shows 4 diff-badges (IDENTITY-RESOLVED, CROSS-SOURCE, REVENUE-LINKED, CONFLICT-AWARE). Verified V17.

### V17 Audit Findings (2026-10-01) — SUPPRESS Hero + V16-Fix Verification + Full Brand Sweep (UX-REVIEW-V17)

**Scope:** 2894-line app (index.html, 9 pages) + app.py backend (v12.0.0). Covers V16-UX-FIX-1/2/3 verification, APP-SUPPRESS-HERO audit, and full 9-dimension brand sweep.

**V16-UX-FIX VERIFICATION (3/3 PASS):**
- V16-UX-FIX-1 (KPI minmax): L1398 = `minmax(200px, 1fr)`. PASS.
- V16-UX-FIX-2 (ErrorCard): L1379-1384 = `error-card` + `role="alert"` + retry. PASS.
- V16-UX-FIX-3 (diff-badges): L1421-1427 = 4 diff-badges. PASS.

**VERSION SYNC (PASS):** Footer v12.0 (L2881) matches app.py 12.0.0 (L121, L2079).

**APP-SUPPRESS-HERO (L2523-2634) — 7/9 PASS, 2 ISSUES:**
- PASS: Color semantics (danger palette, contextual card bg), accessibility (role="alert" + aria-label, bar role="img", table caption + table-scroll), table structure, responsive grid, brand voice, differentiator positioning, component patterns.
- [x] **V17-UX-FIX-1 (Medium):** RESOLVED — L2549 updated to minmax(200px, 1fr) and gap:16. Verified V18.
- [x] **V17-UX-FIX-2 (Medium):** RESOLVED — L2418 suppressError state, L2433 catches to sentinel, L2437-2442 routes to setSuppressError, L2531-2537 inline ErrorCard with role="alert" + Retry. Verified V18.

**FULL BRAND SWEEP (2894 lines):** PASS — all CSS tokens, typography, iconography, landmarks, WCAG compliance holding. 23 captions, 17 role="img", 14 aria-live, 24 table-scroll wrappers.

**KNOWN DEFERRED (Low — acceptable for demo):**
- V12-UX-FIX-3: Naive vs True ROI uses global naive

### V21 Audit Findings (2026-10-02) — Closed-Loop Measurement Page (UX-REVIEW-V21)

**Scope:** 3142-line app (index.html, 10 pages) + app.py backend (v14.0.0). Covers APP-CLOSEDLOOP (new 10th page: Closed-Loop Measurement, L2816-3044, 229 lines).

**PASS (7/9 dimensions):** Color palette (all CSS tokens, zero hardcoded hex, correct semantics), Typography (11/12/13/14/15/22/28 sizes, 500/600/700 weights), Layout (kpi-grid minmax 200px, grid-2, 8px grid), Iconography (refreshCw Lucide SVG), Responsive (auto-fit, table-scroll, flex-wrap), Brand Voice (data-first, "CustomerLake advantage" callout), Backend Security (try/catch, HTTPException, parameterized catalog).

**Version Sync: PASS** — Footer v14.0 (L3129) = app.py 14.0.0 (L121) = health 14.0.0 (L2230).

**FAIL — 4 issues filed to @app-developer:**
- [x] **V21-UX-FIX-1 (Medium):** RESOLVED — All 3 Closed-Loop tables now have `<caption>`: Counterfactual Impact (L3320), Risk-Based Targeting (L3394), ML Optimization Loop (L3425). Verified V32.
- [x] **V21-UX-FIX-2 (Medium):** RESOLVED — L3214 loading state now has `role="status"` + `aria-live="polite"`. Verified V32.
- [x] **V21-UX-FIX-3 (Medium):** RESOLVED — L3367 journey funnel bars now have `role="img"` + `aria-label="{stage}: {pct}%"`. Verified V32.
- [x] **V21-UX-FIX-4 (Low):** RESOLVED — L3237-3241 now shows CAUSAL, COUNTERFACTUAL, CROSS-SOURCE, LOOP diff-badges. Verified V32.

### V22 Audit Findings (2026-10-02) — Revenue-First Hero + Full Brand Sweep (UX-REVIEW-V22)

**Scope:** 3165-line app (index.html, 10 pages) + app.py backend (v15.0.0). Covers APP-REVENUE-FIRST (new Revenue-First Hero Row, L507-529, CMO-43) and V21-fix status verification.

**Revenue-First Hero Section (L507-529) — 8/8 PASS:**
- Color tokens: --success (Total Revenue), --danger (Revenue at Risk), --accent (Marketing Reach), --info (Collection Rate)
- KPI structure: kpi-grid, kpi-card, kpi-label, kpi-value, kpi-sub — standard component pattern
- 8px grid: padding 16px 20px, gap 16px, borderLeft 3px — within tolerance
- Typography: 12px uppercase label, 28px/700 value, 12px secondary sub — matches §3 type scale
- Graceful loading: fallback to '—' when LTV data unavailable
- Revenue-first layout: Business outcomes above engineering metrics (CMO-43)
- Grid layout: minmax(200px, 1fr) auto-fit — matches §5 rule
- Version sync: app.py v15.0.0 ↔ footer v15.0 — PASS

**V21-FIX VERIFICATION:** 4/4 NOT YET APPLIED (all still 'open' in Lakebase). Reiterated priority.

**FAIL — 1 medium issue filed to @app-developer:**
- [x] **V22-UX-FIX-1 (Medium):** RESOLVED — L3233 now uses `<div className="page-header">`. Verified V32.

---

## Revision History

### Legacy Warning Banner (v1.1)
- Container: `--warning-bg` background, 1px `--warning` border, `--radius-lg`, 12px/16px padding
- Layout: flex, center-aligned, 10px gap
- Icon: alertCircle, 16px
- Strong text: `--warning` color, bold
- Body text: `--text-secondary` color, 13px
- Usage: Mark deprecated data sources or views that have been superseded by newer endpoints

---

## Revision History

| Date | Version | Author | Changes |
|---|---|---|---|
| 2026-09-29 | 1.1 | @designer | V3/V4 review: marked A1-A11 RESOLVED, added A12 (bar chart a11y), added channel data-viz palette, added OMNICHANNEL/COST+ROI diff-badges, added legacy warning banner component, updated implementation checklist |
| 2026-09-29 | 1.0 | @designer | Initial brand guidelines, full UX audit of CustomerLake app |
| 2026-09-29 | 1.2.1 | @designer | V7 review: marked A13/A14/A16 RESOLVED, updated §11 checklist (V4-UX-FIX-1/2/3/4, prefers-reduced-motion all checked), filed V7-UX-FIX-1/2/3 |
| 2026-09-30 | 1.2.2 | @designer | V8 review: marked A15 RESOLVED (line 1521 has role="note" on methodology banner). Filed V8-UX-FIX-1 (activation bar charts a11y), V8-UX-FIX-2 (ML table captions), V8-UX-FIX-3 (ML loading aria-live), V8-UX-FIX-4 (footer v7.0 + tab flexWrap). All compliance issues now ≤medium severity. |
| 2026-09-30 | 1.2.3 | @designer | V9 review: ALL V8-UX-FIX-1/2/3/4 verified applied. Full brand audit PASS across all dimensions. WCAG 2.1 AA fully compliant — zero outstanding issues. Filed V9-UX-FIX-1 (ML diff-badge card) and V9-UX-FIX-2 (styled error toast). |
| 2026-09-30 | 1.3.1 | @designer | V11 Incrementality Dashboard audit: PASS with 3 minor issues (1 medium, 2 low). Added 4 diff-badge terms (HOLDOUT, TRUE ROI, RISK-TIER, SUPPRESS). Filed V11-UX-FIX-1/2/3. All V8/V9 fixes verified holding. |
| 2026-09-30 | 1.3.2 | @designer | V12 Dedicated Incrementality Review (UX-REVIEW-INCR-DASH): PASS — 9-dimension audit, all V11 fixes verified. 2 medium issues (purpose row keyboard a11y WCAG 2.1.1, lift sign bug), 2 low (ROI scale, FastAPI version). Filed V12-UX-FIX-1/2/3/4. |
| 2026-09-30 | 1.4.0 | @designer | **V13 FIX VERIFICATION + DASHBOARD AUDIT.** V12-UX-FIX-1/2/4 all verified applied (keyboard a11y, lift sign, FastAPI version). Audited CustomerLake Executive Overview dashboard (2 pages, 18 widgets): titles PASS, data governance PASS, missing descriptions FAIL (§10.5), missing differentiator callouts FAIL (§10.6). Filed DASH-UX-FIX-1/2/3 to @data-analyst. App fully brand-compliant. |
| 2026-10-01 | 1.6.0 | @designer | **V16 IDENTITY REVENUE IMPACT REVIEW + FULL BRAND SWEEP.** V14-UX-FIX-1 verified (version sync). APP-IDENTITY-REV audited (9/9 checks pass: loading a11y, role="note" banner, semantic colors, bar chart a11y, table structure, badge semantics, typography, layout, security). 3 low issues filed: V16-UX-FIX-1 (KPI minmax 180→200px), V16-UX-FIX-2 (silent error→ErrorCard), V16-UX-FIX-3 (add diff-badges). Full 9-dimension brand sweep on 2762 lines: PASS. |
| 2026-10-01 | 1.7.0 | @designer | **V17 SUPPRESS HERO AUDIT + V16-FIX VERIFICATION + FULL BRAND SWEEP.** V16-UX-FIX-1/2/3 all verified applied (KPI minmax, ErrorCard, diff-badges). APP-SUPPRESS-HERO audited (L2523-2634): 7/9 checks pass (color semantics, a11y, tables, responsive, brand voice, differentiator, components). 2 medium issues filed: V17-UX-FIX-1 (hero KPI minmax 160→200px + gap 12→16), V17-UX-FIX-2 (suppress hero silent failure→ErrorCard). Version sync PASS (footer v12.0 = app.py 12.0.0). Full sweep on 2894 lines: PASS. |
| 2026-10-01 | 1.8.0 | @designer | **V18 FULL VERIFICATION.** V17-UX-FIX-1/2 both verified applied (SUPPRESS hero KPI grid + ErrorCard). Dashboard DASH-UX-FIX-1 verified (all 20 widget descriptions applied). DASH-UX-FIX-2 verified (differentiator callouts on both pages: "Why CustomerLake?" + "CustomerLake Activation Edge"). Full 9/9 brand sweep on 2894 lines: PASS. Zero open remediation items for @app-developer. App + Dashboard fully brand-compliant. |
| 2026-10-02 | 1.9.1 | @designer | **V22 REVENUE-FIRST HERO AUDIT + FULL BRAND SWEEP.** Revenue-First Hero Row (L507-529, CMO-43) audited: 8/8 brand dimensions PASS (color tokens, KPI structure, 8px grid, typography, graceful loading, revenue-first layout, grid layout, version sync). V21 fixes verified still pending (4/4 open). 1 medium issue filed: V22-UX-FIX-1 (Closed-Loop page-header class missing). Full 10-page sweep on 3165 lines: PASS. Version sync v15.0 = 15.0.0. |
| 2026-10-02 | 1.9.0 | @designer | **V21 CLOSED-LOOP MEASUREMENT PAGE AUDIT.** New 10th page (APP-CLOSEDLOOP, L2816-3044, 229 lines) audited against v1.8.0 brand spec. 9-dimension review: 7/9 PASS (color, typography, layout, iconography, responsive, brand voice, security). 3 medium a11y issues: 3 tables missing `<caption>` (WCAG 1.3.1), loading state missing `role="status"` + `aria-live` (WCAG 4.1.3), journey funnel bars missing `role="img"` + `aria-label` (WCAG 1.1.1). 1 low enhancement: no diff-badges. Filed V21-UX-FIX-1/2/3/4 to @app-developer. Version sync PASS (v14.0 = 14.0.0). 4 backend endpoints verified. |
### V32 Audit Findings (2026-10-04) — Full Brand Sweep v26.0.0 (UX-REVIEW-V32)

**Scope:** 4760-line app (index.html, 15 pages) + app.py backend (v26.0.0). Covers verification of all 7 open V21-V24 fixes, full audit of 4 new/updated pages (Cost & ROI, Why CustomerLake, Dark Audience Explorer, Statistical Confidence), and 9-dimension brand sweep.

**V21-V24 FIX VERIFICATION (7/7 PASS):**
- V21-UX-FIX-1: 3 Closed-Loop tables now have `<caption>` (L3320, L3394, L3425). PASS.
- V21-UX-FIX-2: Closed-Loop loading state has `role="status"` + `aria-live="polite"` (L3214). PASS.
- V21-UX-FIX-3: Journey funnel bars have `role="img"` + `aria-label` (L3367). PASS.
- V21-UX-FIX-4: Diff-badges present (L3237-3241: CAUSAL, COUNTERFACTUAL, CROSS-SOURCE, LOOP). PASS.
- V22-UX-FIX-1: Closed-Loop page-header class (L3233). PASS.
- V24-UX-FIX-1: Stat Confidence hero KPIs `fontWeight:700` (L3674). PASS.
- V24-UX-FIX-2: Channel propensity grid `auto-fit` (L3754). PASS.

**VERSION SYNC (PASS):** Footer v26.0.0 (L4745) matches app.py 26.0.0 (L126).

**NEW PAGE AUDITS:**

**CostRoiPage (L3928-4138) — 9/9 PASS:**
- page-header (L3963), diff-badges (L3970-3974: TCO-TRANSPARENT, COST-PER-ENTITY, ML-CHANNEL-WASTE, CFO-READY), loading (L3951: role+aria), ErrorCard (L3952), hero KPIs (L3980: minmax 200px, fontSize 36 within 32-36 spec), channel waste table caption (L4058), cost bars role="img"+aria-label (L4017), methodology role="note" (L4129).

**ComparisonPage (L4140-4351) — 9/9 PASS:**
- page-header (L4184), diff-badges (L4189-4193: LIVE DATA, COMPETITOR COMPARISON, ZERO MARKETING CLAIMS, EVERY NUMBER QUERYABLE), loading (L4172: role+aria), ErrorCard (L4173), feature matrix table caption (L4228), ROI waterfall bars role="img"+aria-label (L4320), grid minmax 200px (L4196).

**DarkAudiencePage (L4354-4651) — 7/9 PASS, 2 MEDIUM ISSUES:**
- PASS: page-header (L4436), diff-badges (L4466-4471), loading (L4387: role+aria), ErrorCard (L4388), KPI hero grid (L4442: 4-col), differentiator card (L4466), color semantics, typography, responsive.
- [ ] **V32-UX-FIX-1 (Medium):** Priority Breakdown table (L4477) missing `<caption>`. WCAG 1.3.1. Add: `<caption className="kpi-label" style={{captionSide:'top', textAlign:'left', marginBottom:'8px'}}>Dark audience priority breakdown by LTV tier and churn risk</caption>`
- [ ] **V32-UX-FIX-2 (Medium):** Action Segments table (L4514) missing `<caption>`. WCAG 1.3.1. Add: `<caption className="kpi-label" style={{captionSide:'top', textAlign:'left', marginBottom:'8px'}}>Action segments: dark audience reasons, entity counts, and recoverable revenue</caption>`

**StatisticalConfidencePage (L3598-3925) — PASS (previously audited V24, updates verified):**
- V24-UX-FIX-1/2 verified holding. Power Analysis Transparency section (L3729-3813) fully accessible. Sample Safeguard alert (L3799) has role="alert".

**FULL BRAND SWEEP (4760 lines, 15 pages):**
- All CSS tokens match brand palette. Zero hardcoded hex in components.
- Typography: system font stack, correct type scale across all pages.
- Iconography: 20 Lucide SVG icons, all via Icon component with aria-hidden.
- Layout: 8px grid, sidebar 240px, all cards/KPIs/tables match spec.
- Responsive: Mobile hamburger + Escape handler, auto-fit grids, table-scroll wrappers.
- Accessibility: All nav items are `<button>`, focus-visible outlines, focus trap on panels, aria-live on loading states, prefers-reduced-motion, `<nav>`/`<main>` landmarks.
- Brand voice: Professional, data-first, differentiator badges on all 15 pages.
- Backend security: Parameterized queries, try/catch, HTTPException.

**KNOWN DEFERRED (Low — acceptable for demo):**
- V10-UX-FIX-2: Attribution purposes all use badge-success
- V10-UX-FIX-3: Steward dismiss ✕ Unicode instead of SVG
- V12-UX-FIX-3: Naive vs True ROI uses global naive
- DASH-UX-FIX-3: Dashboard bar chart axis labels
- Loading skeleton screens (Low polish)
- Empty state illustrations (Low polish)

| 2026-10-02 | 1.9.3 | @designer | **V24 FULL BRAND SWEEP (v18.0).** APP-UNUSED-VIEWS (v17: ML Channel Propensity, Statistical Quality Disclosure, Holdout Data Quality) all 9/9 PASS. APP-STAT-CONFIDENCE (v18: new 11th page, 237 lines, 3 endpoints) 8/9 PASS. All V21/V22/V23 fixes verified holding. 2 low issues filed: V24-UX-FIX-1 (fontWeight:800→700 on Stat Confidence hero KPIs), V24-UX-FIX-2 (Channel Propensity grid auto-fill→auto-fit + 140→160px). 4 diff-badge terms added to §6: CAUSAL, P-VALUE, CONFIDENCE-INTERVAL, STATISTICAL-POWER. Version sync v18.0 = 18.0.0 PASS. 11/11 pages brand-compliant. |
| 2026-10-04 | 1.9.6 | @designer | **V32 FULL BRAND SWEEP (v26.0.0, 4760 lines, 15 pages).** All 7 open V21-V24 fixes VERIFIED APPLIED (V21-UX-FIX-1/2/3/4, V22-UX-FIX-1, V24-UX-FIX-1/2). 4 new pages audited: Cost & ROI (9/9 PASS), Why CustomerLake (9/9 PASS), Dark Audience Explorer (7/9 PASS — 2 tables missing captions), Statistical Confidence updates (PASS). Version sync v26.0.0 = 26.0.0 PASS. Filed V32-UX-FIX-1/2 (Dark Audience table captions) to @app-developer. |
| 2026-09-30 | 1.3.0 | @designer | **V10 COMPREHENSIVE AUDIT — FULL PASS.** 9-dimension review (Color, Typography, Iconography, Layout, Components, Responsive, Accessibility, Brand Voice, Security). All 2,310 lines of index.html and 78K of app.py verified against brand spec. WCAG 2.1 AA: all 16 compliance items (A1–A16) confirmed in code. All UX-FIX-1 through V8-UX-FIX-4 verified. 3 low-priority polish items filed: V10-UX-FIX-1 (footer version stale v7.0→v10.0), V10-UX-FIX-2 (attribution badge semantic color), V10-UX-FIX-3 (steward dismiss icon consistency). |
