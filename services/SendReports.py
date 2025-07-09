import maill
async def Send_Reports(logger):
    logger.info("Sending email for reports...!")
    subject = "📊 Daily XB Bhiwandi Report"
    body = f"""
            Hi Dragon🐉,
            Attached daily report for the XB Bhiwandi.
            For a detailed view, please refer to the attached report image(s).

            Regards,  
            Your Automated Monitor Bot
        """
    isattachments = True  # Assuming you want to attach images

    await maill.send_emaill(body, subject, logger, isattachments)