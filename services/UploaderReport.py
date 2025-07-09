import matplotlib.pyplot as plt
import matplotlib.cm as cm
from datetime import datetime, timedelta
import pandas as pd


async def GenerateUploaderReport(logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    df = pd.read_csv('ApiCall.csv')
    df.head()


    # Create a figure with 2 rows and 2 columns (4 subplots)
    fig = plt.figure(figsize=(22, 18))
    gs = fig.add_gridspec(2, 2)

    calapi_counts = df[df['Type2'] == 'CalibrationApi']['ResponseCode'].value_counts()
    callabels = calapi_counts.index.tolist()
    caltotal = calapi_counts.sum()


    # Generate dynamic colors
    cmap = plt.cm.Set2  # or try: plt.cm.Paired, plt.cm.tab10, etc.
    colors = cmap(range(len(callabels)))  # generate distinct colors
    # Setup plot and gridspec

    # Add pie chart
    def autopct_func(pct):
            count = int(round(pct * caltotal / 100.0))
            return f'{pct:.1f}%\n({count})'
    ax1 = fig.add_subplot(gs[0, 0])
    wedges, texts, autotexts = ax1.pie(
            calapi_counts,
            autopct=autopct_func,
            colors=colors,
            startangle=60,
            wedgeprops=dict(width=0.4),  # This makes it a donut
            pctdistance=0.8,
            textprops=dict(color="black", fontweight='bold', fontsize=10)
    )

        # Optional: Add a center label
    ax1.legend(wedges, callabels, loc='center', bbox_to_anchor=(0.8, 0.1))
    ax1.set_title(f"Calibration Data (Total: {caltotal})", fontsize=14, fontweight='bold')


    #2nd piechart

    inscanapi_counts = df[df['Type2'] == 'InscanApi']['ResponseCode'].value_counts()
    inscanlabels = inscanapi_counts.index.tolist()
    inscantotal = inscanapi_counts.sum()


    # Generate dynamic colors
    cmap = plt.cm.Set2  # or try: plt.cm.Paired, plt.cm.tab10, etc.
    colors = cmap(range(len(inscanlabels)))  # generate distinct colors
    # Setup plot and gridspec

    # Add pie chart
    def autopct_func(pct):
            count = int(round(pct * inscantotal / 100.0))
            return f'{pct:.1f}%\n({count})'
    ax2 = fig.add_subplot(gs[0, 1])
    wedges, texts, autotexts = ax2.pie(
            inscanapi_counts,
            autopct=autopct_func,
            colors=colors,
            startangle=60,
            wedgeprops=dict(width=0.4),  # This makes it a donut
            pctdistance=0.8,
            textprops=dict(color="black", fontweight='bold', fontsize=10)
    )

        # Optional: Add a center label
    ax2.legend(wedges, inscanlabels, loc='center', bbox_to_anchor=(0.8, 0.1))
    ax2.set_title(f"Inscan Data (Total: {inscantotal})", fontsize=14, fontweight='bold')


    #2nd piechart

    dwsapi_counts = df[df['Type2'] == 'DWSApi']['ResponseCode'].value_counts()
    dwslabels = dwsapi_counts.index.tolist()
    dwsapitotal = dwsapi_counts.sum()


    # Generate dynamic colors
    cmap = plt.cm.Set2  # or try: plt.cm.Paired, plt.cm.tab10, etc.
    colors = cmap(range(len(dwslabels)))  # generate distinct colors
    # Setup plot and gridspec

    # Add pie chart
    def autopct_func(pct):
            count = int(round(pct * dwsapitotal / 100.0))
            return f'{pct:.1f}%\n({count})'
    ax3 = fig.add_subplot(gs[1, 0])
    wedges, texts, autotexts = ax3.pie(
            dwsapi_counts,
            autopct=autopct_func,
            colors=colors,
            startangle=60,
            wedgeprops=dict(width=0.4),  # This makes it a donut
            pctdistance=0.8,
            textprops=dict(color="black", fontweight='bold', fontsize=10)
    )

        # Optional: Add a center label
    ax3.legend(wedges, dwslabels, loc='center', bbox_to_anchor=(0.8, 0.1))
    ax3.set_title(f"DWS Data (Total: {dwsapitotal})", fontsize=14, fontweight='bold')


    #4th piechart
    df1 = pd.read_csv('AWSCall.csv')
    df1.head()
    if not df1.empty:
            aws_counts = df1[df1['Type2'] == 'ImageUpload']['ResponseCode'].value_counts()
            awslabels = aws_counts.index.tolist()
            awstotal = aws_counts.sum()


            # Generate dynamic colors
            cmap = plt.cm.Set2  # or try: plt.cm.Paired, plt.cm.tab10, etc.
            colors = cmap(range(len(awslabels)))  # generate distinct colors
            # Setup plot and gridspec

            # Add pie chart
            def autopct_func(pct):
                    count = int(round(pct * awstotal / 100.0))
                    return f'{pct:.1f}%\n({count})'
            ax4 = fig.add_subplot(gs[1, 1])
            wedges, texts, autotexts = ax4.pie(
                    aws_counts,
                    autopct=autopct_func,
                    colors=colors,
                    startangle=60,
                    wedgeprops=dict(width=0.4),  # This makes it a donut
                    pctdistance=0.8,
                    textprops=dict(color="black", fontweight='bold', fontsize=10)
            )

            # Optional: Add a center label
            ax4.legend(wedges, awslabels, loc='center', bbox_to_anchor=(0.8, 0.1))
            ax4.set_title(f"AWS Data (Total: {awstotal})", fontsize=14, fontweight='bold')
    if IsTimeRange == True:
        now = end_time_ist
        past_24h = start_time_ist
    else:
        now = datetime.now()
        past_24h = now - timedelta(hours=LastHours)

    fig.suptitle("NIDO-Xpressbees Data Analysis", fontsize=18, fontweight='bold', y=1.02)
    footer_text = f"Generated Report: From {past_24h.strftime('%d-%m-%Y %H:%M:%S')} to {now.strftime('%d-%m-%Y %H:%M:%S')}"
    fig.text(0.5, 0.985, footer_text, ha='center', fontsize=9, color='black')
    left_text = "DataUpload Report"
    fig.text(0.1, 0.995, left_text, ha='center', fontsize=12, color='black', fontweight='bold')
    right_text = "Bhiwandi(Maharashtra)"
    fig.text(0.9, 0.995, right_text, ha='center', fontsize=12, color='black', fontweight='bold')
    plt.tight_layout()
    plt.savefig("uploader.png", dpi=300, bbox_inches='tight')  # or use .pdf/.jpg
    #plt.show()
    logger.info("Uploader Report generated and saved..")

