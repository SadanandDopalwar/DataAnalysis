from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pytz
import SendReports
import asyncio
from threading import Thread


async def GenerateCalibrationReportImage(df, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    # Create a figure with 2 rows and 2 columns (4 subplots)
    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(1, 2)
    xb1_df = df[df['MachineId'] == "d30eb0c6-7d12-43af-a8ee-87e6beb705ff"].copy()
    if xb1_df.empty:
        logger.warning("No data for calibration machines.")
        return

    status_counts = (xb1_df['Final Status'] == 'Passed').value_counts()
    length_counts = (xb1_df['Length Status'] == 'Passed').value_counts()
    width_counts = (xb1_df['Width Status'] == 'Passed').value_counts()
    height_counts = (xb1_df['Height Status'] == 'Passed').value_counts()
    weight_counts = (xb1_df['Weight Status'] == 'Passed').value_counts()

    status_counts = {
        'Passed': (xb1_df['Final Status'] == 'Passed').sum(),
        'Failed': (xb1_df['Final Status'] != 'Passed').sum()
    }

    labels = list(status_counts.keys())
    values = list(status_counts.values())
    total = sum(values)  # Total needed for the percentage function
    colors = ['blue', 'red']
    # Define autopct function
    def autopct_func(pct):
        count = int(round(pct * total / 100.0))
        return f'{pct:.1f}%\n({count})'

    # Add donut chart
    ax1 = fig.add_subplot(gs[0, 0])
    wedges, texts, autotexts = ax1.pie(
        values,
        autopct=autopct_func,
        colors=colors,
        startangle=60,
        wedgeprops=dict(width=0.4),  # This makes it a donut
        pctdistance=0.8,
        textprops=dict(color="black", fontweight='bold', fontsize=10)
    )

    # Optional: Add a center label
    ax1.legend(wedges, labels, loc='center', bbox_to_anchor=(0.8, 0.1))
    ax1.set_title(f"Calibration Data (Total: {total})", fontsize=14, fontweight='bold')
    

    length_failed = (xb1_df['Length Status'] != 'Passed').sum()
    width_failed = (xb1_df['Width Status'] != 'Passed').sum()
    height_failed = (xb1_df['Height Status'] != 'Passed').sum()
    weight_failed = (xb1_df['Weight Status'] != 'Passed').sum()

    categories = ['Length Failed', 'Width Failed', 'Height Failed', 'Weight Failed']
    fail_counts = [length_failed, width_failed, height_failed, weight_failed]
    colors = ['#FF9999', '#FC1C54', '#99CC1A', '#CC99FF'] 

    ax2 = fig.add_subplot(gs[0, 1])
    bars = ax2.bar(categories, fail_counts, color=colors, width=0.5)

    # Title & Labels
    ax2.set_title('Calibration Failed Counts', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Failed Count', fontsize=12)
    ax2.set_facecolor('mintcream')

    # Grid lines
    ax2.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.4)

    # Add count labels above bars
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                height + (height * 0.02),  # dynamically add 2% padding above bar
                f'{int(height)}',
                ha='center',
                va='bottom',
                fontsize=10
            )

    # Rotate x-axis labels for better spacing (optional)
    ax2.set_xticks(range(len(categories)))
    ax2.set_xticklabels(categories, ha='center', fontsize=10)
    # ---------------------------

    if IsTimeRange == True:
        now = end_time_ist
        past_24h = start_time_ist
    else:
        now = datetime.now()
        past_24h = now - timedelta(hours=LastHours)
    fig.suptitle("NIDO-Xpressbees Calibration Data Analysis", fontsize=18, fontweight='bold', y=1.02)
    footer_text = f"Generated Report: From {past_24h.strftime('%d-%m-%Y %H:%M:%S')} to {now.strftime('%d-%m-%Y %H:%M:%S')}"
    fig.text(0.5, 0.985, footer_text, ha='center', fontsize=9, color='black')
    left_text = "Lower-Decker Report"
    fig.text(0.1, 0.995, left_text, ha='center', fontsize=12, color='black', fontweight='bold')
    right_text = "Bhiwandi(Maharashtra)"
    fig.text(0.9, 0.995, right_text, ha='center', fontsize=12, color='black', fontweight='bold')

    plt.tight_layout()
    plt.savefig("xb1calibration_report.png", dpi=300, bbox_inches='tight')  # or use .pdf/.jpg
    #plt.show()

async def GenerateCalibrationReportImage2(df, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    # Create a figure with 2 rows and 2 columns (4 subplots)
    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(1, 2)
    xb2_df = df[df['MachineId'] == "694810c5-8b08-4014-bb02-a2d9c6a7fc77"].copy()
    if xb2_df.empty:
        logger.warning("No data for calibration machines.")
        return

    status_counts = (xb2_df['Final Status'] == 'Passed').value_counts()
    length_counts = (xb2_df['Length Status'] == 'Passed').value_counts()
    width_counts = (xb2_df['Width Status'] == 'Passed').value_counts()
    height_counts = (xb2_df['Height Status'] == 'Passed').value_counts()
    weight_counts = (xb2_df['Weight Status'] == 'Passed').value_counts()

    status_counts = {
        'Passed': (xb2_df['Final Status'] == 'Passed').sum(),
        'Failed': (xb2_df['Final Status'] != 'Passed').sum()
    }

    labels = list(status_counts.keys())
    values = list(status_counts.values())
    total = sum(values)  # Total needed for the percentage function
    colors = ['blue', 'red']
    # Define autopct function
    def autopct_func(pct):
        count = int(round(pct * total / 100.0))
        return f'{pct:.1f}%\n({count})'

    # Add donut chart
    ax1 = fig.add_subplot(gs[0, 0])
    wedges, texts, autotexts = ax1.pie(
        values,
        autopct=autopct_func,
        colors=colors,
        startangle=60,
        wedgeprops=dict(width=0.4),  # This makes it a donut
        pctdistance=0.8,
        textprops=dict(color="black", fontweight='bold', fontsize=10)
    )

    # Optional: Add a center label
    ax1.legend(wedges, labels, loc='center', bbox_to_anchor=(0.8, 0.1))
    ax1.set_title(f"Calibration Data (Total: {total})", fontsize=14, fontweight='bold')
    

    length_failed = (xb2_df['Length Status'] != 'Passed').sum()
    width_failed = (xb2_df['Width Status'] != 'Passed').sum()
    height_failed = (xb2_df['Height Status'] != 'Passed').sum()
    weight_failed = (xb2_df['Weight Status'] != 'Passed').sum()

    categories = ['Length Failed', 'Width Failed', 'Height Failed', 'Weight Failed']
    fail_counts = [length_failed, width_failed, height_failed, weight_failed]
    colors = ['#FF9999', '#FC1C54', '#99CC1A', '#CC99FF'] 

    ax2 = fig.add_subplot(gs[0, 1])
    bars = ax2.bar(categories, fail_counts, color=colors, width=0.5)

    # Title & Labels
    ax2.set_title('Calibration Failed Counts', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Failed Count', fontsize=12)
    ax2.set_facecolor('mintcream')

    # Grid lines
    ax2.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.4)

    # Add count labels above bars
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                height + (height * 0.02),  # dynamically add 2% padding above bar
                f'{int(height)}',
                ha='center',
                va='bottom',
                fontsize=10
            )

    # Rotate x-axis labels for better spacing (optional)
    ax2.set_xticks(range(len(categories)))
    ax2.set_xticklabels(categories, ha='center', fontsize=10)
    # ---------------------------

    if IsTimeRange == True:
        now = end_time_ist
        past_24h = start_time_ist
    else:
        now = datetime.now()
        past_24h = now - timedelta(hours=LastHours)
    fig.suptitle("NIDO-Xpressbees Calibration Data Analysis", fontsize=18, fontweight='bold', y=1.02)
    footer_text = f"Generated Report: From {past_24h.strftime('%d-%m-%Y %H:%M:%S')} to {now.strftime('%d-%m-%Y %H:%M:%S')}"
    fig.text(0.5, 0.985, footer_text, ha='center', fontsize=9, color='black')
    left_text = "Upper-Decker Report"
    fig.text(0.1, 0.995, left_text, ha='center', fontsize=12, color='black', fontweight='bold')
    right_text = "Bhiwandi(Maharashtra)"
    fig.text(0.9, 0.995, right_text, ha='center', fontsize=12, color='black', fontweight='bold')

    plt.tight_layout()
    plt.savefig("xb2calibration_report.png", dpi=300, bbox_inches='tight')  # or use .pdf/.jpg
    #plt.show()
