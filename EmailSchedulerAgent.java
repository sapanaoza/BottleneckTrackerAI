package org.example.Agents;

import jade.core.Agent;
import jakarta.mail.*;
import jakarta.mail.internet.*;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Agent responsible for sending daily email alerts with analysis attachments.
 * It sends an initial alert on startup and continues sending alerts every day at 10:00 AM.
 */
public class EmailSchedulerAgent extends Agent {

    // === CONFIGURATION SECTION ===

    // Sender and recipient email addresses
    private final String from = System.getenv().getOrDefault("EMAIL_FROM", "sapanaoza@gmail.com");
    private final String to = System.getenv().getOrDefault("EMAIL_TO", "sapanaoza@gmail.com");

    // Email credentials – set securely in environment variables
    private final String username = System.getenv("EMAIL_USERNAME"); // Example: sapanaoza@gmail.com
    private final String password = System.getenv("EMAIL_PASSWORD"); // App Password (never hardcode in production!)

    // Path to the CSV file that will be attached in the email
    private final String csvFilePath = System.getenv().getOrDefault("EXPORT_CSV_PATH",
            "C:\\Desktop\\Java\\BottleNeckTrackerAI-GCP\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.csv");

    // Daily interval (24 hours) in milliseconds
    private static final long DAILY_INTERVAL_MS = 24 * 60 * 60 * 1000;

    @Override
    protected void setup() {
        System.out.println(getLocalName() + "Schedules and sends automated email alerts with reports....");

        if (username == null || password == null) {
            System.err.println("EMAIL_USERNAME or EMAIL_PASSWORD environment variables are not set.");
            doDelete();
            return;
        }

        // ⏱️ Send an initial alert email immediately on agent start
        sendDailyAlertEmail();

        // 📆 Schedule the alert email to send daily at 10:00 AM
        scheduleDailyEmail();
    }

    /**
     * Schedule a daily email at exactly 10:00 AM.
     */
    public void scheduleDailyEmail() {
        Timer timer = new Timer();

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 10);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        // ⏳ If 10:00 AM has already passed today, schedule for tomorrow
        if (calendar.getTime().before(new Date())) {
            calendar.add(Calendar.DATE, 1);
        }

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendDailyAlertEmail();
            }
        }, calendar.getTime(), DAILY_INTERVAL_MS);
    }

    /**
     * Build and send the daily alert email with CSV attachment.
     */
    private void sendDailyAlertEmail() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd MMM yyyy, hh:mm a"));

        String subject = "Alert: Machine Performance Requires Review";
        String body = String.format(
                "Dear Team,\n\n" +
                        "BottleneckTrackerAI has detected potential system bottlenecks as of %s.\n\n" +
                        "Alert Status: TRUE\n" +
                        "Attached: Full Analysis Report (CSV)\n\n" +
                        "Please review and take necessary actions.\n\n" +
                        "Regards,\n" +
                        "BottleneckTrackerAI System", timestamp
        );

        sendEmailWithAttachment(subject, body, csvFilePath);
    }

    /**
     * Send an email with subject, body, and attachment file.
     *
     * @param subject Email subject
     * @param body    Email message body
     * @param filePath Path to the file to attach
     */
    public void sendEmailWithAttachment(String subject, String body, String filePath) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        try {
            // Check file existence
            File attachment = new File(filePath);
            if (!attachment.exists()) {
                System.err.println("Attachment file not found: " + filePath);
                return;
            }

            // Construct email
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(subject);

            // Message body
            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(body);

            // Attachment
            MimeBodyPart filePart = new MimeBodyPart();
            filePart.attachFile(attachment);

            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(textPart);
            multipart.addBodyPart(filePart);

            message.setContent(multipart);

            // Send email
            Transport.send(message);
            System.out.println("Email sent successfully with attachment: " + attachment.getName());

        } catch (Exception e) {
            System.err.println("Failed to send email: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
