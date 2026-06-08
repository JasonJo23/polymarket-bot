# Live Excel dashboard - setup guide

Goal: see the bot's PnL and edge in Excel, refreshing by itself, with **no cloud
account**. Three moving parts:

```
 Hetzner server                Your Windows PC                 Excel
 export_csv.py  -- cron -->    pull.bat -- Task Scheduler -->   Power Query
 writes CSVs every 5 min       copies CSVs every 10 min         auto-refresh
```

You only set this up once. After that it runs on its own.

---

## Part 1 - Server: generate the CSVs on a schedule

The exporter (`export_csv.py`) is already in your repo after you pull. It only
reads data, never trades. Test it once on the server:

```bash
cd ~/polymarket-bot
python3 export_csv.py
ls -l excel_export/
```

You should see `summary.csv`, `positions.csv`, `predictions.csv`.

Now schedule it every 5 minutes with cron:

```bash
crontab -e
```

Add this line at the bottom (adjust the path if your bot is not in /root/polymarket-bot):

```
*/5 * * * * cd /root/polymarket-bot && /usr/bin/python3 export_csv.py >> export_csv.log 2>&1
```

Save and exit. The CSVs now refresh on the server every 5 minutes.

---

## Part 2 - Windows: pull the CSVs automatically over SSH

### 2a. One-time: passwordless SSH key (so the pull needs no typing)

In **PowerShell** on your PC:

```powershell
# create a key if you don't already have one (press Enter at every prompt)
ssh-keygen -t ed25519

# copy your public key to the server (enter your server password once)
type $env:USERPROFILE\.ssh\id_ed25519.pub | ssh root@SERVER_IP "mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys"
```

Replace `SERVER_IP` with your server's address (the one you SSH into).
Test it - this should connect **without** asking for a password:

```powershell
ssh root@SERVER_IP "echo connected"
```

### 2b. Create the local data folder and the pull script

```powershell
mkdir C:\PolybotData
```

Create `C:\PolybotData\pull.bat` with this content (Notepad is fine). Replace
`SERVER_IP`:

```bat
@echo off
scp -i %USERPROFILE%\.ssh\id_ed25519 root@SERVER_IP:/root/polymarket-bot/excel_export/*.csv C:\PolybotData\
```

Double-click `pull.bat` once. Three CSV files should appear in `C:\PolybotData`.

### 2c. Run the pull automatically every 10 minutes

In **PowerShell (as Administrator)**:

```powershell
schtasks /create /tn "PolybotPull" /tr "C:\PolybotData\pull.bat" /sc minute /mo 10 /f
```

That's it - your PC now copies the latest CSVs every 10 minutes.
(To stop it later: `schtasks /delete /tn "PolybotPull" /f`)

---

## Part 3 - Excel: connect once, auto-refresh forever

1. Open a new Excel workbook.
2. **Data -> Get Data -> From Text/CSV** -> pick `C:\PolybotData\summary.csv` -> **Load**.
3. Repeat for `positions.csv` and `predictions.csv` (Data -> Get Data -> From Text/CSV).
   You now have three tables on three sheets.
4. Make them refresh by themselves: **Data -> Queries & Connections**, then for
   **each** query right-click -> **Properties** -> tick:
   - "Refresh data when opening the file"
   - "Refresh every [10] minutes"
5. Save as `PolybotDashboard.xlsx`.

Keep the file open and it updates itself. Or hit **Data -> Refresh All** anytime.

### Build the views you want (PivotTables)

The `positions` and `predictions` tables are tidy (one row per record), so Excel
can slice them live:

- Click inside the **positions** table -> **Insert -> PivotTable**.
  - PnL by market type: Rows = `market_type`, Values = Sum of `realized_pnl_usdc`,
    plus Count of `win` and Average of `roi`.
  - Swap `market_type` for `signal_type` or `wallet_source` to see which path earns.
- Click inside **predictions** -> PivotTable.
  - Edge check: Rows = `should_bet`, Values = Average of `roi_if_bet`. If the
    `0` (blocked) row's average beats the `1` (bet) row, the edge gate is costing
    you - time to try EDGE_DETECTOR_SHADOW_MODE.
  - Calibration: Rows = `confidence`, Values = Average of `win` and Average of `brier_claude`.
- PivotTables refresh together with the data (Refresh All), and you can add
  PivotCharts on top for a visual dashboard.

The **summary** sheet is a quick at-a-glance: `net_realized_pnl_usdc`,
`win_rate_pct`, `claude_brier` vs `market_brier`, and `edge_verdict`.

---

## What "live" means here

Worst-case lag from a trade closing to it showing in Excel is about:
server cron (<=5 min) + PC pull (<=10 min) + Excel refresh (<=10 min) ~ up to 25
minutes. Tighten any interval if you want it snappier. It is "almost live", not
millisecond-live - which is exactly right for a strategy you review, not day-trade.

## First-run note

Until the bot has closed a handful of positions and logged some resolved
predictions, the tables will be short or empty and the summary will say
"not enough data". That's expected - it fills in as the bot runs.
