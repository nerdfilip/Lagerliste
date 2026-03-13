// ============================================================================
// CONFIGURATION
// ============================================================================
const UPLOADS_FOLDER_ID = '1ecRiWJON03Pd0qDNpRxNhTNDsYyw614Z';
const READY_FOLDER_ID = '16mMxz1DvsIEgKUk4mAXnamwxIQ50ddP5';

// NEW: Files that crash Google's converter and need in-memory SheetJS parsing
const HEAVY_EXCEL_FILES = [
  "aktionsplan int", 
  "wt stationär", 
  "export pt_de", 
  "ospl_artikelliste",
  "übersicht überschneiderartikel"
];

// ============================================================================
// 1. MENU & UI TRIGGER
// ============================================================================
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Lagerliste Tools')
    .addItem('1. Convert Files to CSV', 'openProgressUI')
    .addItem('2. Import to BigQuery', 'openBQProgressUI')
    .addItem('3. Run Transformations (SQL)', 'openTransformUI')
    .addToUi();
}

function openProgressUI() {
  const html = HtmlService.createHtmlOutputFromFile('ProgressUI')
    .setWidth(600)
    .setHeight(450)
    .setTitle('File Processing Terminal');
  SpreadsheetApp.getUi().showModalDialog(html, 'Automated File Converter');
}

// ============================================================================
// 2. FETCH FILES
// ============================================================================
function getPendingFiles() {
  console.log("[INIT] Scanning 01_Uploads for pending files...");
  const folder = DriveApp.getFolderById(UPLOADS_FOLDER_ID);
  const files = folder.getFiles();
  let fileList = [];
  
  while (files.hasNext()) {
    let f = files.next();
    fileList.push({ id: f.getId(), name: f.getName(), mimeType: f.getMimeType() });
  }
  
  console.log(`[INIT] Found ${fileList.length} files. Passing queue to UI.`);
  return fileList; 
}

// ============================================================================
// 3. PROCESS A SINGLE FILE
// ============================================================================
function processSingleFile(fileObj) {
  const readyFolder = DriveApp.getFolderById(READY_FOLDER_ID);
  const file = DriveApp.getFileById(fileObj.id);
  const lowerName = fileObj.name.toLowerCase();
  
  let logMessage = "";
  let serverTrace = []; 
  
  // Dual-logger: Writes to Apps Script Executions AND the UI array
  function systemLog(msg) {
    console.log(msg);
    serverTrace.push(msg);
  }
  
  systemLog(`[SERVER] --- STARTING FILE: ${fileObj.name} ---`);
  systemLog(`[SERVER] Received ID: ${fileObj.id} | Type: ${fileObj.mimeType}`);
  
  try {
    // --- SCENARIO A: CSV OR GOOGLE SHEET FILE ---
    // Added MimeType.GOOGLE_SHEETS here so it moves directly without conversion
    if (lowerName.endsWith('.csv') || fileObj.mimeType === MimeType.CSV || fileObj.mimeType === 'text/csv' || fileObj.mimeType === MimeType.GOOGLE_SHEETS) {
      systemLog(`[SERVER] Valid CSV or Native Google Sheet detected. Moving directly to 02_Ready.`);
      file.moveTo(readyFolder);
      return { success: true, log: `[SUCCESS] Moved file: ${fileObj.name}`, trace: serverTrace };
    }
    
    // --- SCENARIO B: EXCEL / XLSB FILE ---
    else if (lowerName.endsWith('.xlsx') || lowerName.endsWith('.xls') || lowerName.endsWith('.xlsb') ||
             fileObj.mimeType === MimeType.MICROSOFT_EXCEL || 
             fileObj.mimeType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
             fileObj.mimeType === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12') {
             
      let baseFileName = fileObj.name.replace(/\.(xlsx?|xlsb)$/i, '').trim();
      let csvName = `${baseFileName}.csv`;

      // Check if this file requires our SheetJS bypass engine
      let isHeavyFile = HEAVY_EXCEL_FILES.some(keyword => lowerName.includes(keyword));
      let isXlsb = lowerName.endsWith('.xlsb') || fileObj.mimeType === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12';

      // --- PATH 1: SHEETJS IN-MEMORY PARSING (Heavy files & .xlsb) ---
      if (isHeavyFile || isXlsb) {
        systemLog(`[SERVER] Detected heavy/binary file. Bypassing Drive API.`);
        systemLog(`[SERVER] Booting up SheetJS In-Memory Engine...`);
        
        let csvBlob = convertHeavyExcelWithSheetJS_(fileObj.id, csvName);
        
        systemLog(`[SERVER] In-Memory parsing successful. Writing to 02_Ready folder...`);
        readyFolder.createFile(csvBlob);
        
        systemLog(`[SERVER] Trashing original heavy file...`);
        file.setTrashed(true);
        
        return { success: true, log: `[SUCCESS] SheetJS Converted: ${fileObj.name} -> ${csvName}`, trace: serverTrace };
      } 
      
      // --- PATH 2: STANDARD GOOGLE DRIVE API CONVERSION ---
      else {
        systemLog(`[SERVER] Instructing Google Drive to convert Excel file...`);
        let tempSheetId = convertToGoogleSheet_(file, UPLOADS_FOLDER_ID);
        systemLog(`[SERVER] Drive API success. Temp Sheet ID: ${tempSheetId}`);

        systemLog(`[SERVER] Opening sheet to extract data...`);
        let spreadsheet = SpreadsheetApp.openById(tempSheetId);
        let sheetToExport = spreadsheet.getSheets()[0]; 
        
        systemLog(`[SERVER] Targeted first tab: [${sheetToExport.getName()}]`);

        // --- QUALITY ASSURANCE: CHECK FOR BROKEN FORMULAS ---
        systemLog(`[SERVER] Running QA scan for broken formulas...`);
        let hasError = sheetToExport.createTextFinder("#ERROR!").findNext();
        let hasRef = sheetToExport.createTextFinder("#REF!").findNext();

        if (hasError || hasRef) {
          systemLog(`[SERVER] CRITICAL: Found #ERROR! or #REF! inside file.`);
          DriveApp.getFileById(tempSheetId).setTrashed(true);
          file.setTrashed(true); 
          systemLog(`[SERVER] Trashed temporary sheet and rejected original file.`);
          
          return { 
            success: false, 
            log: `[REJECTED] ${fileObj.name}: Contains broken formulas (#ERROR!). Deleted. Please save as CSV locally and re-upload.`,
            trace: serverTrace
          };
        }
        systemLog(`[SERVER] QA Passed: No broken formulas detected.`);

        // Export as CSV 
        systemLog(`[SERVER] Requesting raw CSV data from Google servers...`);
        let csvBlob = exportSheetAsCsvBlob_(tempSheetId, sheetToExport.getSheetId(), csvName);
        systemLog(`[SERVER] CSV Blob generated. Writing to 02_Ready folder...`);
        readyFolder.createFile(csvBlob);

        // Clean up files
        systemLog(`[SERVER] Cleaning up origin files...`);
        DriveApp.getFileById(tempSheetId).setTrashed(true);
        file.setTrashed(true);
        systemLog(`[SERVER] Cleanup complete. Process finished.`);
        
        return { success: true, log: `[SUCCESS] Converted: ${fileObj.name} -> ${csvName}`, trace: serverTrace };
      }
    }
    
    // --- SCENARIO C: IGNORED FILE ---
    else {
      systemLog(`[SERVER] File type unsupported. Ignoring.`);
      return { success: true, log: `[IGNORED] Unsupported format: ${fileObj.name}`, trace: serverTrace };
    }
    
  } catch (error) {
    systemLog(`[SERVER] SYSTEM CRASH: ${error.message}`);
    if (error.message.includes("Request Too Large")) {
      return { success: false, log: `[ERROR] ${fileObj.name}: File too massive for cloud conversion. MUST be uploaded as CSV.`, trace: serverTrace };
    }
    return { success: false, log: `[ERROR] ${fileObj.name}: ${error.message}`, trace: serverTrace };
  }
}

// ============================================================================
// 4. HELPER FUNCTIONS
// ============================================================================
function convertToGoogleSheet_(excelFile, folderId) {
  let metadata = { name: excelFile.getName(), mimeType: MimeType.GOOGLE_SHEETS, parents: [folderId] };
  let newFile = Drive.Files.copy(metadata, excelFile.getId(), { supportsAllDrives: true });
  return newFile.id;
}

function exportSheetAsCsvBlob_(spreadsheetId, sheetId, fileName) {
  let url = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/export?format=csv&gid=${sheetId}`;
  
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      let response = UrlFetchApp.fetch(url, {
        headers: { 'Authorization': 'Bearer ' + ScriptApp.getOAuthToken() },
        muteHttpExceptions: true
      });
      if (response.getResponseCode() === 200) return response.getBlob().setName(fileName);
    } catch (e) { if (attempt === 3) throw e; }
    Utilities.sleep(2000); 
  }
  throw new Error("Failed to export tab after 3 attempts.");
}

/**
 * Bypasses Google Drive API limits by parsing complex or binary files 
 * completely in-memory using the open-source SheetJS library.
 */
function convertHeavyExcelWithSheetJS_(fileId, csvFileName) {
  // 1. Fetch SheetJS Library via CDN and load it into Apps Script Memory
  const sheetJSUrl = "https://cdn.sheetjs.com/xlsx-0.19.3/package/dist/xlsx.full.min.js";
  const scriptText = UrlFetchApp.fetch(sheetJSUrl).getContentText();
  eval(scriptText); // Executes the library to make 'XLSX' available
  
  // 2. Download the heavy file into memory as a byte array
  const file = DriveApp.getFileById(fileId);
  const bytes = file.getBlob().getBytes();
  
  // 3. Convert Apps Script signed bytes to Unsigned Integer Array for SheetJS
  const u8 = new Uint8Array(bytes);
  
  // 4. Parse the workbook (cellDates: true ensures dates don't turn into integer serials)
  const workbook = XLSX.read(u8, {type: 'array', cellDates: true});
  
  // 5. Target the first sheet
  const firstSheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[firstSheetName];
  
  // 6. Convert the sheet directly to a CSV string
  // FS: "," enforces the comma delimiter BigQuery expects
  const csvString = XLSX.utils.sheet_to_csv(worksheet, {FS: ","}); 
  
  // 7. Package it as a Blob ready to be saved
  return Utilities.newBlob(csvString, MimeType.CSV, csvFileName);
}