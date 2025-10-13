////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Slack Setup.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

function AHA_SlackNotify3(message) {
  const start = new Date();
  try {

  const url = PropertiesService.getScriptProperties().getProperty("SLACK_WEBHOOK_URL");
  if (!url) {
    Logger.log("❌ Webhook URL not found. Run setSlackWebhookUrl() first.");
    return;
  }

  const category = PropertiesService.getScriptProperties().getProperty("CENTRAL_CATEGORY");


  const workerMessage = category + " - " + "*Central*" + " : " + message

  const payload = JSON.stringify({
    text: workerMessage,
    username: "Google Sheets Bot",
    icon_emoji: ":robot_face:"
  });

  const options = {
    method: "POST",
    contentType: "application/json",
    payload: payload,
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  Logger.log("Slack response: " + response.getContentText());

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
