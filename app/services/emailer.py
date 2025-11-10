"""
app/services/emailer.py

Handles email delivery of LinkedIn data export packages.
Supports SMTP with TLS (Gmail, SendGrid, or any SMTP provider).
Sends emails with ZIP attachments containing normalized data.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import Optional, List
from datetime import datetime
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class EmailerService:
    """Handles email delivery with attachments via SMTP."""
    
    def __init__(self):
        """Initialize emailer with SMTP configuration from settings."""
        self.smtp_host = settings.SMTP_HOST
        self.smtp_port = settings.SMTP_PORT
        self.smtp_user = settings.SMTP_USER
        self.smtp_password = settings.SMTP_PASSWORD
        self.from_email = settings.FROM_EMAIL
        self.use_tls = settings.SMTP_USE_TLS
        self.enabled = settings.EMAIL_ENABLED
        
        # Validate configuration
        if self.enabled:
            self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate that required email configuration is present."""
        required_fields = {
            "SMTP_HOST": self.smtp_host,
            "SMTP_PORT": self.smtp_port,
            "SMTP_USER": self.smtp_user,
            "SMTP_PASSWORD": self.smtp_password,
            "FROM_EMAIL": self.from_email,
        }
        
        missing = [key for key, value in required_fields.items() if not value]
        if missing:
            logger.warning(f"Missing email configuration: {', '.join(missing)}")
            logger.warning("Email delivery will be disabled")
            self.enabled = False
    
    def send_data_package(
        self,
        to_emails: List[str],
        zip_path: Path,
        run_id: Optional[str] = None,
        record_counts: Optional[dict] = None,
        subject: Optional[str] = None,
    ) -> bool:
        """
        Send LinkedIn data package via email.
        
        Args:
            to_emails: List of recipient email addresses
            zip_path: Path to ZIP file to attach
            run_id: Optional run identifier for email body
            record_counts: Optional dict of table counts for email body
            subject: Optional custom subject line
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Email is disabled, skipping send")
            return False
        
        if not zip_path.exists():
            logger.error(f"ZIP file not found: {zip_path}")
            return False
        
        # Check file size (most SMTP servers limit to 25MB)
        file_size_mb = zip_path.stat().st_size / (1024 * 1024)
        if file_size_mb > 25:
            logger.error(f"ZIP file too large for email: {file_size_mb:.2f} MB")
            return False
        
        try:
            # Create message
            msg = self._create_message(
                to_emails=to_emails,
                zip_path=zip_path,
                run_id=run_id,
                record_counts=record_counts,
                subject=subject,
            )
            
            # Send via SMTP
            self._send_smtp(msg, to_emails)
            
            logger.info(f"Successfully sent data package to {len(to_emails)} recipient(s)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}", exc_info=True)
            return False
    
    def _create_message(
        self,
        to_emails: List[str],
        zip_path: Path,
        run_id: Optional[str],
        record_counts: Optional[dict],
        subject: Optional[str],
    ) -> MIMEMultipart:
        """
        Create email message with attachment.
        
        Args:
            to_emails: List of recipient email addresses
            zip_path: Path to ZIP attachment
            run_id: Optional run identifier
            record_counts: Optional dict of record counts
            subject: Optional custom subject
            
        Returns:
            Configured MIMEMultipart message
        """
        msg = MIMEMultipart()
        msg['From'] = self.from_email
        msg['To'] = ', '.join(to_emails)
        
        # Subject line
        if subject:
            msg['Subject'] = subject
        else:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            msg['Subject'] = f"LinkedIn Data Export - {timestamp}"
        
        # Email body
        body = self._generate_email_body(zip_path, run_id, record_counts)
        msg.attach(MIMEText(body, 'html'))
        
        # Attach ZIP file
        self._attach_file(msg, zip_path)
        
        return msg
    
    def _generate_email_body(
        self,
        zip_path: Path,
        run_id: Optional[str],
        record_counts: Optional[dict],
    ) -> str:
        """
        Generate HTML email body.
        
        Args:
            zip_path: Path to ZIP file
            run_id: Optional run identifier
            record_counts: Optional dict of record counts
            
        Returns:
            HTML email body string
        """
        file_size_mb = zip_path.stat().st_size / (1024 * 1024)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background-color: #0077B5; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .info-box {{ background-color: #f4f4f4; border-left: 4px solid #0077B5; padding: 15px; margin: 20px 0; }}
                .stats-table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                .stats-table th, .stats-table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                .stats-table th {{ background-color: #0077B5; color: white; }}
                .footer {{ text-align: center; color: #666; font-size: 12px; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>LinkedIn Data Export Package</h1>
            </div>
            <div class="content">
                <p>Your LinkedIn data has been successfully processed and normalized.</p>
                
                <div class="info-box">
                    <strong>Package Details:</strong><br>
                    • Generated: {timestamp}<br>
                    • File: {zip_path.name}<br>
                    • Size: {file_size_mb:.2f} MB<br>
        """
        
        if run_id:
            html += f"                    • Run ID: {run_id}<br>\n"
        
        html += """
                </div>
        """
        
        # Add record counts if provided
        if record_counts:
            html += """
                <h3>Data Summary</h3>
                <table class="stats-table">
                    <thead>
                        <tr>
                            <th>Table</th>
                            <th>Records</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for table_name, count in sorted(record_counts.items()):
                html += f"""
                        <tr>
                            <td>{table_name}</td>
                            <td>{count:,}</td>
                        </tr>
                """
            
            html += """
                    </tbody>
                </table>
            """
        
        html += """
                <h3>Package Contents</h3>
                <p>The attached ZIP file contains:</p>
                <ul>
                    <li><strong>CSV files</strong> - One per table for easy import into spreadsheets or databases</li>
                    <li><strong>JSON files</strong> - Structured data for programmatic access</li>
                    <li><strong>manifest.json</strong> - Metadata about this export</li>
                </ul>
                
                <div class="info-box">
                    <strong>Note:</strong> This is an automated email from the LinkedIn Data Ingestor system. 
                    The data has been normalized and validated before packaging.
                </div>
            </div>
            
            <div class="footer">
                <p>LinkedIn Data Ingestor System<br>
                Automated Data Processing Pipeline</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _attach_file(self, msg: MIMEMultipart, file_path: Path) -> None:
        """
        Attach file to email message.
        
        Args:
            msg: MIMEMultipart message to attach to
            file_path: Path to file to attach
        """
        with open(file_path, 'rb') as f:
            part = MIMEBase('application', 'zip')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {file_path.name}'
            )
            msg.attach(part)
    
    def _send_smtp(self, msg: MIMEMultipart, to_emails: List[str]) -> None:
        """
        Send email via SMTP server.
        
        Args:
            msg: Prepared MIMEMultipart message
            to_emails: List of recipient email addresses
            
        Raises:
            smtplib.SMTPException: If SMTP operation fails
        """
        logger.info(f"Connecting to SMTP server: {self.smtp_host}:{self.smtp_port}")
        
        # Create SMTP connection
        if self.use_tls:
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls()
        else:
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
        
        try:
            # Login
            if self.smtp_user and self.smtp_password:
                logger.debug(f"Logging in as: {self.smtp_user}")
                server.login(self.smtp_user, self.smtp_password)
            
            # Send email
            server.send_message(msg)
            logger.info(f"Email sent successfully to: {', '.join(to_emails)}")
            
        finally:
            server.quit()
    
    def send_error_notification(
        self,
        to_emails: List[str],
        error_message: str,
        run_id: Optional[str] = None,
    ) -> bool:
        """
        Send error notification email without attachment.
        
        Args:
            to_emails: List of recipient email addresses
            error_message: Error description
            run_id: Optional run identifier
            
        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Email is disabled, skipping error notification")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = "LinkedIn Data Ingestor - Error Alert"
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                    .header {{ background-color: #dc3545; color: white; padding: 20px; text-align: center; }}
                    .content {{ padding: 20px; }}
                    .error-box {{ background-color: #f8d7da; border-left: 4px solid #dc3545; padding: 15px; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>⚠️ LinkedIn Data Ingestor Error</h1>
                </div>
                <div class="content">
                    <p>An error occurred during the data ingestion process.</p>
                    
                    <div class="error-box">
                        <strong>Error Details:</strong><br>
                        • Time: {timestamp}<br>
            """
            
            if run_id:
                body += f"            • Run ID: {run_id}<br>\n"
            
            body += f"""
                        • Message: {error_message}
                    </div>
                    
                    <p>Please check the application logs for more details.</p>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(body, 'html'))
            
            self._send_smtp(msg, to_emails)
            logger.info("Error notification email sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}", exc_info=True)
            return False
    
    def test_connection(self) -> bool:
        """
        Test SMTP connection and authentication.
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.enabled:
            logger.warning("Email is disabled")
            return False
        
        try:
            logger.info("Testing SMTP connection...")
            
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10)
                server.starttls()
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10)
            
            if self.smtp_user and self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            
            server.quit()
            logger.info("SMTP connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}", exc_info=True)
            return False


# Singleton instance
emailer_service = EmailerService()