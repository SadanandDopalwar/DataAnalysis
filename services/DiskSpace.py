import psutil
import asyncio
import maill



async def check_disk_usage(disk_path, DiskCheckCutoff, IsSendMail, MailRecipients, logger):
    # Get disk usage statistics for the specified mount point
    disk_usage = psutil.disk_usage(disk_path)

    total = disk_usage.total / (1024 ** 3)  # Convert bytes to GB
    used = disk_usage.used / (1024 ** 3)    # Convert bytes to GB
    free = disk_usage.free / (1024 ** 3)    # Convert bytes to GB
    percent_used = disk_usage.percent
    
    DiskData = f"Disk Usage for '{disk_path}':"
    TotalSpace = f"Total space: {total:.2f} GB"
    UsedSpace = f"Used space: {used:.2f} GB"
    FreeSpace = f"Free space: {free:.2f} GB"
    PercentageUsed = f"Percentage used: {percent_used:.2f}%"
    print(f"Disk Usage for '{disk_path}':")
    print(f"Total space: {total:.2f} GB")
    print(f"Used space: {used:.2f} GB")
    print(f"Free space: {free:.2f} GB")
    print(f"Percentage used: {percent_used}%")
    
    if percent_used > DiskCheckCutoff:
        #print(f"Warning: Disk usage is above {DiskCheckCutoff}%!")
        logger.warning(f"Disk usage is above {DiskCheckCutoff}% for {disk_path}!")
        if IsSendMail:
            try:
                logger.info("Sending email...")
                subject = "🚨 XB Bhiwandi Alert: Storage Status Update"
                body = f"""
                        Hi Dragon🐉,

                        Please check the XB Bhiwandi server space details below:

                        -------------------------------
                        📁 Disk Information:
                        {DiskData}

                        🧮 Total Space     : {TotalSpace}
                        📂 Free Space      : {FreeSpace}
                        💾 Used Space      : {UsedSpace}
                        📊 Percentage Used : {PercentageUsed}
                        -------------------------------

                        Regards,  
                        Your Automated Monitor Bot
                    """


                await maill.send_emaill(body, subject, logger, isattachments=False)
            except Exception as e:
                logger.error(f"Error sending email: {e}")
        

# Example usage for the root directory

#check_disk_usage('/')  # Replace with the disk or partition you want to check