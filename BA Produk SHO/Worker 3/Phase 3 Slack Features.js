////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Slack Features.gs 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

function AHA_ListFilesInMoveFolder3() {
  const rootId = "0AJyZWtXd1795Uk9PVA"; // Your shared drive root ID
  const moveFolderName = "Move";
  const maxFilesToShow = 20; // Optional: limit to avoid Slack length limits

  const root = DriveApp.getFolderById(rootId);
  const moveFolders = root.getFoldersByName(moveFolderName);

  if (!moveFolders.hasNext()) {
    return `❌ Folder "${moveFolderName}" not found in shared drive.`;
  }

  const moveFolder = moveFolders.next();
  const files = moveFolder.getFiles();

  let fileList = [];
  let count = 0;

  while (files.hasNext()) {
    const file = files.next();
    count++;
    fileList.push(`${count}. ${file.getName()}`);
    if (count >= maxFilesToShow) break;
  }

  if (fileList.length === 0) {
    AHA_SlackNotify3(`📁 No files currently in the *${moveFolderName}* folder.`);
  }

  const moreNote = count >= maxFilesToShow ? `\n_...and more files exist_` : ``;
  AHA_SlackNotify3(`📦 *Files in '${moveFolderName}' folder*:\n` + fileList.join("\n") + moreNote);
}

function AHA_ListArchivedFiles3(category, dateFolder) {
  const ARCHIVE_FOLDER_ID = "1S3u9qZr3Hf5M8Z5HXhFR1QFVDMO41SFm"; // Replace with your actual Archive folder ID
  const archiveFolder = DriveApp.getFolderById(ARCHIVE_FOLDER_ID);

  const categoryFolders = archiveFolder.getFoldersByName(category);
  if (!categoryFolders.hasNext()) {
    return [`❌ Category folder '${category}' not found.`];
  }
  const categoryFolder = categoryFolders.next();

  const dateFolders = categoryFolder.getFoldersByName(dateFolder);
  if (!dateFolders.hasNext()) {
    return [`❌ Date folder '${dateFolder}' not found in '${category}'`];
  }
  const targetFolder = dateFolders.next();
  const folderUrl = targetFolder.getUrl();
  const files = targetFolder.getFiles();

  const fileList = [
    `📂 Category: ${category}`,
    `📅 Date: ${dateFolder}`,
    `🔗 Folder link: ${folderUrl}`,
    ""
  ];

  while (files.hasNext()) {
    const file = files.next();
    fileList.push("• " + file.getName());
  }

  return fileList.length > 4
    ? fileList
    : [...fileList, "⚠️ No files found in this folder."];
}

















