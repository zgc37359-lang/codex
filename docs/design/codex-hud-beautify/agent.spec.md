# Codex HUD Beautify — Agent Specification

## Requirements

### Functional Requirements
1. **Remove timestamp** from Row 1 panel line (`status_hud_panel_line`)
2. **Update badge style** — label in muted gray + smaller, value in white bold, with subtle color-tinted background
3. **Simplify OPS in Row 4** — replace per-type breakdown with single process count: `⚙ N processes`
4. **Update meter bar characters** — use `█` and `░` instead of `=` and `.`
5. **Reduce row accent colors** — current palette has 9 colors; consolidate to 4 row-dominant colors

### Non-Functional
- No changes to backend state logic
- No changes to snapshot tests (update separately with `UPDATE_EXPECT=1`)
- Preserve animation behavior (spinner/pulse markers)
- Preserve meter color semantics (green=good, yellow=warning, red=critical)

## Constraints

- Must work with existing Ratatui 0.29.0 (nornagon fork)
- `Color::Rgb` used throughout, not ANSI palette indices
- Terminal theme-adaptive approach from existing code should be preserved

## Approach

Modify `tui/src/chatwidget/status_surfaces.rs`:
1. Update color constants to refined 4-color palette
2. Modify `hud_push_badge` and `hud_push_meter` to apply new label/value styling
3. Update `status_hud_panel_line` to remove timestamp and dynamic marker from non-Row-1 lines
4. Update `status_hud_activity_line` to show simplified process count
5. Update `hud_push_bar` to use `█`/`░` characters

## Key Files

| File | Changes |
|------|---------|
| `tui/src/chatwidget/status_surfaces.rs` | Color constants, badge rendering, meter characters, activity line |

## Color Palette

```rust
// Row 1 — Sky Blue
const HUD_SKY: Color = Color::Rgb(96, 165, 250);       // primary
const HUD_SKY_BG: Color = Color::Rgb(15, 23, 42);    // deep tint bg
const HUD_SKY_LABEL: Color = Color::Rgb(100, 116, 139); // muted label

// Row 2 — Violet
const HUD_VIOLET: Color = Color::Rgb(167, 139, 250);

// Row 3 — Mint
const HUD_MINT: Color = Color::Rgb(52, 211, 153);

// Row 4 — Amber
const HUD_AMBER: Color = Color::Rgb(251, 146, 60);

// Shared
const HUD_ROSE: Color = Color::Rgb(251, 113, 133);   // error/critical
const HUD_GOLD: Color = Color::Rgb(250, 204, 21);    // warning
const HUD_INK: Color = Color::Rgb(15, 23, 42);       // badge text on colored bg
const HUD_WHITE: Color = Color::Rgb(248, 250, 252);   // value text
const HUD_MUTED: Color = Color::Rgb(100, 116, 139);  // label text
```

## Badge Style

```rust
// Label: muted gray, small, non-bold
Style::default().fg(HUD_MUTED)

// Value: white, bold
Style::default().fg(HUD_WHITE).add_modifier(Modifier::BOLD)

// Badge background: subtle color tint
Style::default().fg(row_color).on(row_color_bg)
```

## Meter Characters

```rust
// Before: "=" for filled, "." for empty
// After: "█" for filled, "░" for empty
```

## Activity Line Simplification

```rust
// Before: "cmd {}  exec {}  spawn {}  crew {}"
// After: "⚙ {} processes"  // single aggregated count
```
