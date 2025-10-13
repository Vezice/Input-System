////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Slack Setup.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Mac Sync

// WORKER SENSITIVE
function AHA_SlackNotify3(message) {
  const start = new Date();
  try {
    const url = PropertiesService.getScriptProperties().getProperty("SLACK_WEBHOOK_URL");
    if (!url) {
      Logger.log("❌ Webhook URL not found. Run setSlackWebhookUrl() first.");
      return;
    }
    const workerCount = PropertiesService.getScriptProperties().getProperty("WORKER_COUNT");
    const category = PropertiesService.getScriptProperties().getProperty("WORKER_CATEGORY");

    const workerMessage = category + " - " + workerCount + " : " + message;

    const payload = JSON.stringify({
      text: workerMessage,
      username: "Google Sheets Bot",
      icon_emoji: ":robot_face:"
    });

    const options = {
      method: "POST",
      contentType: "application/json",
      payload: payload,
      muteHttpExceptions: false // Set to false to allow catch block to trigger
    };

    // Wrap the UrlFetchApp call in the retry helper
    AHA_ExecuteWithRetry(() => {
      const response = UrlFetchApp.fetch(url, options);
      Logger.log("Slack response: " + response.getContentText());
    }, 'Send Slack Notification', 3, 1000); // Retry 3 times, starting with a 1-second delay

  } catch (err) {
    // The retry helper will log the final failure, but we can log here too.
    Logger.log(`❌ Slack notification failed permanently: ${err.message}`);
  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}

function AHA_TestSendMessage3(){
  const start = new Date();
  try {

  AHA_SlackNotify3("✅ Webhook Connected from *Apps Script*!");

  } finally {
    const end = new Date();
    AHA_LogRuntime3(end - start);
  }
}