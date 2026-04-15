# Codex HUD Beautify Specification

**Date:** 2026-04-14
**Status:** Draft

## Context

The current HUD (4-line status display) works but feels visually cluttered: too many borders, timestamp noise, oversized labels, and fragmented color usage. User wants a cleaner dashboard aesthetic with color as the breakthrough point.

## Decision

Adopt a **dashboard panel** style: multi-line blocks with full borders and meter bars, but refined colors and typography that create visual hierarchy without clutter.

## Design Direction

### Visual Style
- Multi-line panel layout (4 rows), full Unicode box-drawing borders
- Dynamic marker (● / spinner) only on Row 1
- No timestamp in Row 1
- Meter bars with fill/empty segments (█ / ░ style)

### Color System
Each row has one dominant color, used consistently across label badge, meter, and accent:

| Row | Content | Dominant Color |
|-----|---------|----------------|
| Row 1 | Mode + State | Sky Blue `#60A5FA` |
| Row 2 | Session + Git + MCP | Violet `#A78BFA` |
| Row 3 | Context + Tokens Usage | Mint `#34D399` |
| Row 4 | Rate Limits + Processes | Amber `#FB923C` |

Label text: muted gray (low visual weight)
Value text: bright white + bold
Badge background: dominant color at low saturation (deep tint)

### Badge Typography
- Labels: muted gray, small, non-bold (e.g., `MODEL`)
- Values: white, bold (e.g., `gpt-5.3-codex`)
- Labels and values in `LABEL VALUE` format within a subtle-colored badge background

### Row 1 — Mode/State
`● MODEL gpt-5.3-codex · MIND medium · TIER fast · MODE Default · STATE Ready`
- Leftmost: dynamic marker (● or spinner), color matches STATE semantic color
- STATE colors: Ready=Mint, Working=Sky, Thinking=Gold, Waiting=Violet
- No timestamp

### Row 2 — Session
`THREAD dev-session · a1b2c3 · SPACE project @ ~/code · GIT feature-xyz · MCP 2/3 ready 1 failed`
- No dynamic marker
- MCP failed count shown in coral/red

### Row 3 — Usage
`CTX ████████░░░ 67% 1.2M/4M · TOKENS 500K in · 200K out · 50K reason`
- Meter bar with fill (█) and empty (░) blocks
- Percentage as part of meter line, not separate field

### Row 4 — Activity
`5H ████████░░ 80% @reset · WEEK ██████████ 100% · ⚙ 3 processes`
- Simplified OPS: only total process count, no per-type breakdown
- Process count in amber when active, muted when idle

## Out of Scope
- Changes to snapshot tests (will be updated via `UPDATE_EXPECT=1`)
- Backend/state logic changes — HUD is purely rendering

## Open Questions

## Success Criteria
- [ ] 4-row HUD renders with cleaner visual hierarchy
- [ ] Color grouping is immediately readable at a glance
- [ ] Meter bars are visually prominent
- [ ] Process count simplified to single number
- [ ] No timestamp noise in Row 1
