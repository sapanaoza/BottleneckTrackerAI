package org.example.Agents;

import jade.core.Agent;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.Multipart;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class EmailSchedulerAgent extends Agent {

    // Sender email address (your Gmail)
    private final String from = "yourmail@gmail.com";
    // Recipient email address (can be the same or different)
    private final String to = "yourmail@gmail.com";

    // Path to the CSV file attachment
    private final String csvFilePath = "C:\\Users\\Falgun\\OneDrive\\Desktop\\Personal\\Java\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.csv";

    @Override
    protected void setup() {
        System.out.println(getLocalName() + " started. Initializing email scheduler...");

        // ✅ Immediately send an alert email when the agent starts
        sendDailyAlertEmail();

        // 🕒 Schedule to send alert emails daily at 10:00 AM
        scheduleDailyEmail();
    }

    /**
     * Schedules a TimerTask to send an email daily at 10:00 AM.
     */
    public void scheduleDailyEmail() {
        Timer timer = new Timer();

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 10);  // Set hour to 10 AM
        calendar.set(Calendar.MINUTE, 0);         // Set minute to 0
        calendar.set(Calendar.SECOND, 0);         // Set second to 0

        // If current time is already past 10 AM, schedule for the next day
        if (calendar.getTime().before(new Date())) {
            calendar.add(Calendar.DATE, 1);
        }

        // Schedule the task to run repeatedly every 24 hours starting at the calculated time
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendDailyAlertEmail();
            }
        }, calendar.getTime(), 24 * 60 * 60 * 1000); // 24 hours in milliseconds
    }

    /**
     * Constructs and sends the alert email with attachment.
     */
    private void sendDailyAlertEmail() {
        // Get current timestamp formatted nicely
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yyyy, hh:mm a");
        String timestamp = now.format(formatter);

        // Email subject and body text
        String subject = "Alert: Machine in the file list requires review, maintenance, or predictive action";
        String body = "Dear Team,\n\n"
                + "BottleneckTrackerAI has detected a potential system bottleneck as of " + timestamp + "\n\n"
                + "Alert Details:\n"
                + "Alert Status: TRUE\n"
                + "Attached: Full Analysis Report (CSV/JSON)\n\n"
                + "Please review and take necessary action.\n\n"
                + "Regards,\n"
                + "BottleneckTrackerAI System";

        // Call helper method to send the email with attachment
        sendEmailWithAttachment(subject, body, csvFilePath);
    }

    /**
     * Sends an email with the given subject, body, and file attachment.
     *
     * @param subject Email subject line
     * @param body Email body text
     * @param filePath Path to the file to attach
     */
    public void sendEmailWithAttachment(String subject, String body, String filePath) {
        // Gmail username and app password for SMTP authentication
        final String username = "yourmail@gmail.com";
        final String password = "password"; // ⚠️ Use App Password, keep secure and do NOT hardcode in production

        // Setup mail server properties
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");            // Enable SMTP authentication
        props.put("mail.smtp.starttls.enable", "true"); // Enable STARTTLS encryption
        props.put("mail.smtp.host", "smtp.gmail.com");  // Gmail SMTP host
        props.put("mail.smtp.port", "587");              // Gmail SMTP port for TLS

        // Create a mail session with authentication
        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        try {
            // Create a new email message
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));   // Set sender
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to)); // Set recipients
            message.setSubject(subject);                   // Set subject

            // Create body part for the email text content
            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(body);

            // Create body part for the file attachment
            MimeBodyPart filePart = new MimeBodyPart();
            filePart.attachFile(new File(filePath));

            // Combine text and attachment into a multipart email
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(textPart);
            multipart.addBodyPart(filePart);

            // Set the content of the email to multipart
            message.setContent(multipart);

            // Send the email
            Transport.send(message);
            System.out.println(" Email sent successfully with attachment!");

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(" Failed to send email: " + e.getMessage());
        }
    }
}
