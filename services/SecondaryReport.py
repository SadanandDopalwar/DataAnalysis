from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pytz
import SendReports
import asyncio
from threading import Thread




async def GenerateSecondaryReport(df, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    # Create a figure with 2 rows and 2 columns (4 subplots)
    fig = plt.figure(figsize=(18, 15))
    gs = fig.add_gridspec(3, 2)
    secondary_df = df[df['Machine Type'] == "Secondary"].copy()
    if secondary_df.empty:
        logger.warning("No data for Secondary machines.")
        return
    status_counts = (secondary_df['Secondary Status'] == False).value_counts()
    category_counts = secondary_df['Secondary Rejection Detail'].value_counts()

    #Filtering on both conditions
    bay_counts = secondary_df[
        (secondary_df['Secondary Sorted Bin'] != "R1001") & 
        (secondary_df['Secondary Status'] == False)
    ]['Secondary Sorted Bin'].value_counts()
    # --- Chart 1: Status == 'BAY1' Pie Chart ---
    labels = ['REJECTED' if val is False else 'SORTED' for val in status_counts.index]

    # Define color mapping
    color_map = {True: 'blue', False: 'red'}
    colors = [color_map[val] for val in status_counts.index]

    total = status_counts.sum()

    # Custom autopct function to show both percentage and count
    def autopct_func(pct):
        count = int(round(pct * total / 100.0))
        return f'{pct:.1f}%\n({count})'
    ax1 = fig.add_subplot(gs[0, 0])
    wedges, texts, autotexts = ax1.pie(
        status_counts,
        autopct=autopct_func,
        colors=colors,
        startangle=60,
        wedgeprops=dict(width=0.4),  # This makes it a donut
        pctdistance=0.8,
        textprops=dict(color="black", fontweight='bold', fontsize=10)
    )

    # Optional: Add a center label
    ax1.legend(wedges, labels, loc='center', bbox_to_anchor=(0.9, 0.09))
    ax1.set_title(f"Secondary Infeed Data (Total: {total})", fontsize=14, fontweight='bold')
    # --- Chart 2: Hourly Throughput ---

    secondary_df['Scan Timestamp'] = pd.to_datetime(secondary_df['Scan Timestamp'], format='%d-%m-%Y %H:%M:%S')

    # Extract hour (this gives only the hour part: 0 to 23)
    secondary_df['Hour'] = secondary_df['Scan Timestamp'].dt.hour

    # Group by hour and count entries
    all_hours = range(24)
    hourly_counts = secondary_df['Hour'].value_counts().reindex(all_hours, fill_value=0).sort_index()
    # Normalize the counts to range between 0 and 1 for colormap
    norm = plt.Normalize(hourly_counts.min(), hourly_counts.max())
    colors = cm.viridis(norm(hourly_counts.values))
    # hourly_counts = df['Hour'].value_counts().sort_index()
    ax2 = fig.add_subplot(gs[0, 1])

    # Plot line (run chart)
    ax2.plot(hourly_counts.index, hourly_counts.values, marker='o', color='teal', linewidth=2)

    # Set titles and labels
    ax2.set_title("Hourly Scan Throughput", fontsize=14, fontweight='bold')
    ax2.set_xlabel("Hour of Day", fontsize=12)
    ax2.set_ylabel("Scan Count", fontsize=12)
    ax2.set_xticks(range(24))
    ax2.set_facecolor('#f5f5f5')
    ax2.grid(True, color='gray', linestyle='--', linewidth=0.5, alpha=0.3)

    active_values = hourly_counts[hourly_counts > 0].values
    average_throughput = int(round(active_values.mean())) if len(active_values) > 0 else 0
    max_index = hourly_counts.values.argmax()
    max_hour = hourly_counts.index[max_index]
    max_value = hourly_counts.values[max_index]

    # Add average line
    ax2.axhline(
        average_throughput,
        color='blue',
        linestyle='--',
        linewidth=1.2,
        alpha=0.7,
        label=f'Avg: {average_throughput}'
    )
    #ax2.text(23, average_throughput + 0.5, f'Avg: {average_throughput}', color='blue', fontsize=9, ha='right')
    ax2.plot(
        max_hour, max_value,
        'o',
        color='black',
        markersize=6,
        markeredgecolor='black',
        markeredgewidth=0.8,
        label=f'Max: {max_value}'
    )
    # Mark max point with a red dot and annotation
    ax2.plot(max_hour, max_value, 'o', color='black', markersize=6, markeredgecolor='black', markeredgewidth=0.8)

    for i, value in enumerate(hourly_counts.values):
        if value > 0:
            ax2.text(i, value + 0.8, str(value), ha='center', va='bottom', fontsize=8)

    # Show legend
    ax2.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0.)


    # --- Chart 3: Rejection Category Bar Chart ---
    ax3 = fig.add_subplot(gs[1, :])
    norm2 = plt.Normalize(category_counts.min(), category_counts.max())
    colors2 = cm.viridis(norm2(category_counts.values))
    ax3.bar(category_counts.index, category_counts.values, color=colors2)

    ax3.set_title("Secondary Rejection Data", fontsize=14, fontweight='bold')
    ax3.set_xlabel("Category", fontsize=12)
    ax3.set_ylabel("Count", fontsize=12)

    # Improve ticks
    ax3.set_facecolor('mintcream')
    # ax2.grid(False)  # Turn off grid lines
    ax3.grid(True, color='gray', linestyle='--', linewidth=0.5, alpha=0.3)

    for i, count in enumerate(category_counts.values):
        if count > 0:
            ax3.text(i, count + 0.5, str(count), ha='center', va='bottom', fontsize=9)


    # --- Chart 4: Status == 'BAY1' or Primary Sort Summary Table ---



    bay_counts = bay_counts.sort_index()

    # Normalize colors based on values
    norm = plt.Normalize(bay_counts.min(), bay_counts.max())
    colors = cm.viridis(norm(bay_counts.values))
    num_bars = len(bay_counts)

    # Set dynamic bar width based on number of bars
    bar_width = min(0.8, max(0.5, 10 / num_bars))  # clamp width between 0.1 and 0.8

    # Set dynamic font size for bar labels and x-axis ticks
    font_size_bar_label = max(5, min(6, 20 / num_bars))
    font_size_xticks = max(5, min(12, 40 / num_bars))

    # Plot bars with dynamic width

    # Create subplot
    ax4 = fig.add_subplot(gs[2, :])
    bars = ax4.bar(bay_counts.index, bay_counts.values, color=colors, width=bar_width)
    ax4.set_title("Secondary Sorted", fontsize=14, fontweight='bold')
    ax4.set_xlabel("Bins", fontsize=12)
    ax4.set_ylabel("Sorted Count", fontsize=12)

    # Improve ticks
    ax4.set_facecolor('mintcream')
    # ax2.grid(False)  # Turn off grid lines
    ax4.grid(True, color='gray', linestyle='--', linewidth=0.5, alpha=0.3)

    # Change count text size and position on top of bars
    # Add count labels above bars
    max_count = max(bay_counts.values)  # or bay_counts.max() if it's a Pandas Series
    offset = max_count * 0.01  # dynamic vertical spacing
    
    # Add count text on top of bars with dynamic font size
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax4.text(
                bar.get_x() + bar.get_width() / 2,  # center of bar
                height + offset,
                str(int(height)),
                ha='center',
                va='bottom',
                fontsize=font_size_bar_label,
                color='black',
                rotation = 60
            )


    # Set x-axis ticks and rotate with dynamic font size
    ax4.set_xticks(range(num_bars))
    ax4.set_xticklabels(bay_counts.index, rotation=60, fontsize=font_size_xticks)

    if IsTimeRange == True:
        now = end_time_ist
        past_24h = start_time_ist
    else:
        now = datetime.now()
        past_24h = now - timedelta(hours=LastHours)

    # Format the footer
    fig.suptitle("NIDO-Xpressbees Data Analysis", fontsize=18, fontweight='bold', y=1.02)
    footer_text = f"Generated Report: From {past_24h.strftime('%d-%m-%Y %H:%M:%S')} to {now.strftime('%d-%m-%Y %H:%M:%S')}"
    fig.text(0.5, 0.985, footer_text, ha='center', fontsize=9, color='black')
    left_text = "Secondary App Report"
    fig.text(0.1, 0.995, left_text, ha='center', fontsize=12, color='black', fontweight='bold')
    right_text = "Bhiwandi(Maharashtra)"
    fig.text(0.9, 0.995, right_text, ha='center', fontsize=12, color='black', fontweight='bold')
    plt.tight_layout()
    plt.savefig("secondaryreport.png", dpi=300, bbox_inches='tight')  # or use .pdf/.jpg
    #plt.show()
    logger.info("Secondary Report generated and saved..")