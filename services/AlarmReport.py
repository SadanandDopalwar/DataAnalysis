from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pytz
import SendReports
import asyncio
from threading import Thread



async def GenerateAlarmReport(alarm_df, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    try:
        alarm_df['Raised Timestamp'] = pd.to_datetime(
            alarm_df['Raised Timestamp'], format='%d-%m-%Y %H:%M:%S', errors='coerce'
        )

        # Fix duration format for parsing
        def fix_alarm_time_format(val):
            if pd.isna(val):
                return pd.NaT
            parts = val.split(":")
            if len(parts) == 2:
                minutes = parts[0]
                seconds = parts[1]
                if '.' in seconds:
                    sec, ms = seconds.split(".")
                    ms = ms.ljust(3, '0')
                    seconds = f"{sec}.{ms}"
                return f"00:{minutes}:{seconds}"
            return val

        alarm_df['Fixed Alarm Time'] = alarm_df['Total Alarm Time(hh:mm:ss.fff)'].apply(fix_alarm_time_format)

        # Convert to timedelta
        alarm_df['Total Alarm Time'] = pd.to_timedelta(alarm_df['Fixed Alarm Time'], errors='coerce')

        # Split sources
        machine_alarms = alarm_df[alarm_df['Source'] == 'Machine']
        it_alarms = alarm_df[alarm_df['Source'] == 'IT']

        # Top 10 Machine Alarms
        top_machine = machine_alarms.groupby("ShortDescription")["Total Alarm Time"] \
            .sum().sort_values(ascending=False).head(5)
        top_machine_sec = top_machine.dt.total_seconds()

        # Top 10 IT Alarms
        top_it = it_alarms.groupby("ShortDescription")["Total Alarm Time"] \
            .sum().sort_values(ascending=False).head(5)
        top_it_sec = top_it.dt.total_seconds()

        # Plotting
        fig, axes = plt.subplots(2, 1, figsize=(16, 12))

        # --- Machine Alarms (Horizontal) ---
        colors_machine = cm.viridis(top_machine_sec / top_machine_sec.max())
        axes[0].barh(top_machine_sec.index, top_machine_sec.values, color=colors_machine)
        axes[0].set_title("Top 5 Machine Alarms by Duration", fontsize=14, fontweight='bold')
        axes[0].set_xlabel("Duration (seconds)")
        axes[0].invert_yaxis()  # highest duration at top
        axes[0].grid(True, linestyle='--', alpha=0.3)

        # Add labels on bars
        for i, (label, value) in enumerate(zip(top_machine_sec.index, top_machine_sec.values)):
            axes[0].annotate(f"{value:.1f}s", xy=(value, i), va='center', ha='left', fontsize=10, color='black')

        # --- IT Alarms (Horizontal) ---
        colors_it = cm.inferno(top_it_sec / top_it_sec.max())
        axes[1].barh(top_it_sec.index, top_it_sec.values, color=colors_it)
        axes[1].set_title("Top 5 IT Alarms by Duration", fontsize=14, fontweight='bold')
        axes[1].set_xlabel("Duration (seconds)")
        axes[1].invert_yaxis()
        axes[1].grid(True, linestyle='--', alpha=0.3)

        # Add labels on bars
        for i, (label, value) in enumerate(zip(top_it_sec.index, top_it_sec.values)):
            axes[1].annotate(f"{value:.1f}s", xy=(value, i), va='center', ha='left', fontsize=10, color='black')


        if IsTimeRange == True:
            now = end_time_ist
            past_24h = start_time_ist
        else:
            now = datetime.now()
            past_24h = now - timedelta(hours=LastHours)
        fig.suptitle("NIDO-Xpressbees Data Analysis", fontsize=18, fontweight='bold', y=1.02)
        footer_text = f"Generated Report: From {past_24h.strftime('%d-%m-%Y %H:%M:%S')} to {now.strftime('%d-%m-%Y %H:%M:%S')}"
        fig.text(0.5, 0.985, footer_text, ha='center', fontsize=9, color='black')
        left_text = "Alarm Report"
        fig.text(0.1, 0.995, left_text, ha='center', fontsize=12, color='black', fontweight='bold')
        right_text = "Bhiwandi(Maharashtra)"
        fig.text(0.9, 0.995, right_text, ha='center', fontsize=12, color='black', fontweight='bold')
        plt.tight_layout()
        plt.savefig("alarmreport.png", dpi=300, bbox_inches='tight')  # or use .pdf/.jpg
        #plt.show()
        logger.info("Alarm Report generated and saved..")
        
    except Exception as e:
        logger.error(f"Error generating alarm report: {e}")