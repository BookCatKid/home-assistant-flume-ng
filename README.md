# Flume NG for Home Assistant

[![HACS validation](https://github.com/BookCatKid/home-assistant-flume-ng/actions/workflows/validate.yml/badge.svg)](https://github.com/BookCatKid/home-assistant-flume-ng/actions/workflows/validate.yml)
[![PyPI](https://img.shields.io/pypi/v/PyFlumeNG.svg)](https://pypi.org/project/PyFlumeNG/)

**Flume NG** is an expanded Home Assistant integration for Flume water monitors. It keeps the familiar Flume usage and diagnostic entities while adding the management and portal features that are not currently exposed by Home Assistant's built-in `flume` integration.

Flume NG uses the separate `flume_ng` domain, so it can be installed alongside Home Assistant's built-in `flume` integration while you evaluate it.

## Highlights

- Live current flow and normal Flume usage sensors.
- Portal-aware fast polling with automatic rate-limit backoff.
- Notification event entity and `flume_notification` Home Assistant bus events.
- Usage-alert rule switches and create/update/delete actions.
- Do Not Alert schedules, current schedule status, and per-rule mute switches.
- Budget used, target, remaining, and percentage sensors plus budget management actions.
- Portal-style usage breakdown including Outdoor, Indoor, Shower, Toilet, Clothes Washer, Dishwasher, and dynamically discovered categories.
- Email and iOS notification preference switches while preserving unknown Flume preference bits.
- Usage-alert history and richer notification data including duration and average flow when Flume provides a query window.
- Usage-span discovery and relabeling.
- Away Mode and supported shutoff controls.
- Shared-access, emergency-contact, location-profile, and meter-accuracy management/diagnostics.
- Hardened read-only expert API action with secret redaction.

## Requirements

- Home Assistant **2026.8.0 or newer**.
- A Flume account and Flume water monitor.
- Internet access from Home Assistant to the Flume API.

The integration installs [PyFlumeNG](https://github.com/BookCatKid/PyFlumeNG) from PyPI automatically.

## Installation

### Home Assistant 2026.11 or newer

1. Open **Settings → Marketplace** and search for **Flume NG**.
2. If it isn't listed, select **Add from link**, enter `https://github.com/BookCatKid/home-assistant-flume-ng`, and select **Add**. Connect GitHub if prompted.
3. Open **Flume NG** in the Marketplace and select **Install**. Restart Home Assistant if prompted.

If you installed Flume NG through HACS before upgrading to 2026.11, the Marketplace imports it automatically. You don't need to reinstall it.

### Home Assistant 2026.8 through 2026.10

1. Open **HACS** and select **Custom repositories** from the menu.
2. Add `https://github.com/BookCatKid/home-assistant-flume-ng` as an **Integration** repository.
3. Install **Flume NG** and restart Home Assistant.

### Set up Flume NG

Go to **Settings → Devices & services → Add integration**, search for **Flume NG**, and follow the prompts.

If you installed Flume NG manually, remove the manually copied `custom_components/flume_ng` folder before installing it through Marketplace or HACS. Leave the integration entry in **Devices & services** in place to keep your configuration.

## Authentication modes

### Portal authentication — recommended

Portal authentication uses the same Flume customer-portal account flow and exposes the full Flume NG feature set: alert rules, Do Not Alert schedules, budgets, notification preferences, usage breakdown, management actions, and fast quota-aware polling.

The Flume portal currently advertises a substantially larger API quota than the Personal API. Flume NG reads the server's rate-limit headers and only uses fast polling while the available quota is healthy; it automatically backs off if the reported quota becomes low.

### Personal API authentication

Existing Personal API client credentials remain supported. This mode is useful for the traditional usage/current-flow functionality, but portal-only controls are not available.

## Polling behavior

When Portal authentication is reporting the expected high quota and at least 5% remains:

| Data | Portal auth | Personal/fallback |
| --- | ---: | ---: |
| Live current flow | 15 seconds | 60 seconds |
| Notifications | 15 seconds | 5 minutes |
| Standard usage sensors | 60 seconds | 60 seconds |
| Portal rules, schedules, budgets, preferences | 60 seconds | 15-minute fallback when fast polling is unavailable |
| Usage breakdown | 60 seconds | 15-minute fallback when fast polling is unavailable |
| Connectivity/battery | 60 minutes | 60 minutes |

The intervals are implementation defaults, not promises from Flume, and may change if Flume changes its quota behavior.

## Important entities

Some commonly useful entities are:

- **Live Current Flow** — direct active-flow reading.
- **High Flow** binary sensor — reflects a High Flow Alert notification; it is not a raw flow-threshold detector.
- **High Flow Alert** switch — enables/disables the actual Flume High Flow Alert rule.
- **Grass Watering** (or another Do Not Alert schedule) binary sensor — on only while that schedule window is currently active.
- **`<rule>: mute during <schedule>`** switches — control whether a Do Not Alert schedule suppresses that specific usage-alert rule.
- **Monthly Budget / Target / Remaining / Percentage** — current budget status.
- **Usage Breakdown Total** and category sensors — current-month classified usage.
- **API Requests Remaining** — latest quota information reported by Flume.

Entity names are derived from your Flume device, rules, schedules, and classifications, so exact entity IDs can vary.

## Actions

Flume NG exposes Home Assistant actions for the operations that do not map cleanly to entities, including:

- Listing/creating/updating/deleting usage-alert rules.
- Listing/creating/updating/deleting Do Not Alert schedules.
- Associating schedules with alert rules.
- Listing/creating/updating/deleting budgets.
- Reading and updating notification preferences.
- Reading usage-alert history.
- Listing usage spans and valid classification types, then relabeling a span.
- Listing/granting/revoking shared location access.
- Listing/creating/updating/deleting emergency contacts.
- Reading meter-accuracy and location-profile diagnostics.
- Listing the allowlisted read-only expert API surface.

The normal actions use typed Home Assistant fields wherever practical. The expert API action intentionally blocks write/credential operations and recursively redacts sensitive response fields.

## Notification events

New Flume notifications are emitted through both the Flume NG event entity and the Home Assistant event bus as `flume_notification` events. Existing notification history is used only as the startup baseline and is **not replayed** as a burst of new events when Home Assistant starts.

Where the notification contains Flume's usage query window, Flume NG can enrich the event with fields such as duration, average GPM, and estimated gallons used during the event.

## Coexisting with the built-in Flume integration

Flume NG intentionally uses the unique domain `flume_ng`; it does **not** override `homeassistant.components.flume`. You may keep the built-in integration configured while testing Flume NG. Doing so will duplicate some read-only usage entities and API traffic, so most users will eventually want to keep only the integration they actually use.

## Portal API note

The advanced functionality is based on the API behavior used by Flume's own customer web portal. That surface is not presented by Flume as a stable third-party public API, so Flume can change it without notice. The integration is deliberately defensive around capabilities, validation, quotas, and secrets, but portal changes can still require an update.

## Support

Please report Flume NG integration issues in this repository's [issue tracker](https://github.com/BookCatKid/home-assistant-flume-ng/issues). PyFlumeNG library/API issues can be reported at [BookCatKid/PyFlumeNG](https://github.com/BookCatKid/PyFlumeNG/issues).

When reporting a problem, include your Home Assistant version, Flume NG version, authentication mode, and relevant Home Assistant logs. **Do not post passwords, OAuth tokens, API client secrets, notification push tokens, or other credentials.**

## Attribution and trademark

Flume NG builds on Home Assistant's existing Flume integration and uses the Flume brand assets already maintained by the Home Assistant brands project. The integration code remains under the Apache License 2.0.

Flume and related marks belong to their respective owner. This community project is not affiliated with or endorsed by Flume.
