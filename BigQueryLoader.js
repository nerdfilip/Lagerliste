// ============================================================================
// CONFIGURATION: BIGQUERY & FOLDERS
// ============================================================================
const GCP_PROJECT_ID = 'sit-ldl-int-oi-a-lvzt-run-818b'; 
const DATASET_ID = 'lagerliste_imports'; 
const ARCHIVE_FOLDER_ID = '1IOrUiTS_xXb69EBUcb8rqPSbUhQKjugO'; 
// const READY_FOLDER_ID = '16mMxz1DvsIEgKUk4mAXnamwxIQ50ddP5';

const FILE_RULES = [
  { keyword: "db abfrage",                     headerRow: 1, dataRow: 2, delimiter: ";" },
  { keyword: "übersicht überschneiderartikel", headerRow: 2, dataRow: 3 },
  { keyword: "bäf_de",                         headerRow: 7, dataRow: 9 }, 
  { keyword: "osnl",                           headerRow: 1, dataRow: 2 },
  { keyword: "rwa",                            headerRow: 3, dataRow: 4 }
];

const TYPE_OVERRIDES = {
  "ian": "INT64",
  "laenderspezifische_sap_nummern": "INT64",
  "abverkaufshorizont_nat": "INT64",
  "kopfartikel": "INT64",
  "summe_von_st_rwa": "NUMERIC",
  "aktions_vk": "NUMERIC",
  "sortiment_vk_lidl": "NUMERIC", 
};

// --- UPDATED: Now fetches both CSV and Google Sheets ---
function getReadyFiles() {
  const folder = DriveApp.getFolderById(READY_FOLDER_ID);
  const files = folder.getFiles();
  let fileList = [];
  
  while (files.hasNext()) {
    let f = files.next();
    let mime = f.getMimeType();
    
    if (mime === MimeType.CSV || mime === MimeType.GOOGLE_SHEETS || mime === 'text/csv') {
      fileList.push({ id: f.getId(), name: f.getName(), mimeType: mime });
    }
  }
  return fileList; 
}

// ============================================================================
// 1. UI TRIGGER & UTILITIES
// ============================================================================
function openBQProgressUI() {
  const html = HtmlService.createHtmlOutputFromFile('BQProgressUI')
    .setWidth(600)
    .setHeight(450)
    .setTitle('BigQuery Ingestion Terminal');
  SpreadsheetApp.getUi().showModalDialog(html, 'Database Loader');
}

function cleanTableName(fileName) {
  // Strip common extensions just in case
  let raw = fileName.replace(/\.csv$/i, '').replace(/\.xlsx?$/i, '').replace(/\.xlsb$/i, '');
  const map = { 'ä':'ae', 'ö':'oe', 'ü':'ue', 'Ä':'ae', 'Ö':'oe', 'Ü':'ue', 'ß':'ss' };
  let en = raw.replace(/[äöüÄÖÜß]/g, m => map[m]);
  return 'raw_' + en.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
}

// ============================================================================
// 2. SCHEMA DETECTOR WITH SMART PROFILING
// ============================================================================
// --- UPDATED: Added mimeType parameter to handle Google Sheets ---
function buildDynamicSchema(fileId, headerRow, dataRow, forcedDelimiter, projectId, datasetId, mimeType) {
  console.log(`[SCHEMA] Phase 1: Fetching file ID ${fileId} from Drive...`);
  
  let url;
  if (mimeType === MimeType.GOOGLE_SHEETS) {
    // If it's a Google Sheet, we ask Google to export it as a CSV string on the fly
    url = `https://docs.google.com/spreadsheets/d/${fileId}/export?format=csv`;
  } else {
    // Standard CSV media download
    url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`;
  }

  let response = UrlFetchApp.fetch(url, {
    headers: { 'Authorization': 'Bearer ' + ScriptApp.getOAuthToken(), 'Range': 'bytes=0-500000' },
    muteHttpExceptions: true
  });
  
  console.log(`[SCHEMA] Phase 2: Parsing CSV text...`);
  let rawText = response.getContentText();
  let lines = rawText.split(/\r?\n/);
  
  // Smart Delimiter Detection
  let fileDelimiter = forcedDelimiter;
  if (!fileDelimiter) {
    let firstLine = lines[0] || "";
    let commaCount = (firstLine.match(/,/g) || []).length;
    let semiCount = (firstLine.match(/;/g) || []).length;
    fileDelimiter = semiCount > commaCount ? ';' : ',';
    console.log(`[SCHEMA] Auto-detected delimiter: [${fileDelimiter}]`);
  }

  let rawHeaders = [];
  let sampleDataRow = []; 
  try { 
    let parsed = Utilities.parseCsv(rawText, fileDelimiter); 
    rawHeaders = parsed[headerRow - 1] || [];
    sampleDataRow = parsed[dataRow - 1] || []; 
  } catch(e) { 
    rawHeaders = (lines[headerRow - 1] || "").split(fileDelimiter); 
    sampleDataRow = (lines[dataRow - 1] || "").split(fileDelimiter); 
  }

  console.log(`[SCHEMA] Phase 3: Translating headers to BigQuery format...`);
  const map = { 'ä':'ae', 'ö':'oe', 'ü':'ue', 'Ä':'ae', 'Ö':'oe', 'Ü':'ue', 'ß':'ss' };
  let englishHeaders = rawHeaders.map(val => {
    let en = String(val).replace(/[äöüÄÖÜß]/g, m => map[m]);
    en = en.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase().replace(/_+/g, '_').replace(/^_|_$/g, '');
    if (!en || /^[0-9]/.test(en)) en = 'col_' + en;
    return en.substring(0, 290);
  });

  console.log(`[SCHEMA] Phase 4: Deduplicating headers...`);
  let used = new Set();
  for(let i = 0; i < englishHeaders.length; i++) {
    let f = englishHeaders[i];
    let c = 1;
    while(used.has(f) && c < 500) { f = englishHeaders[i] + '_' + c; c++; }
    used.add(f);
    englishHeaders[i] = f;
  }

  console.log(`[SCHEMA] Phase 5: Smart Type Coercion & Overrides...`);
  let finalSchemaFields = [];
  
  for(let i = 0; i < englishHeaders.length; i++) {
    let colName = englishHeaders[i];
    let sampleVal = (sampleDataRow[i] || '').trim();
    let checkName = colName.toLowerCase();
    let detectedType = 'STRING';

    let looksLikeDate = /^\d{1,2}[\.\/]\d{1,2}[\.\/]\d{4}/.test(sampleVal) || /^\d{4}-\d{2}-\d{2}/.test(sampleVal);
    let soundsLikeDate = checkName.includes('datum');

    let looksLikeCurrency = /[€$£]/.test(sampleVal);
    let soundsLikeCurrency = (checkName.includes('preis') || checkName.includes('wert') || checkName.includes('rwa') || checkName.includes('volumen') || checkName.includes('kosten') || checkName.includes('€') || checkName.includes('eur'));

    if (looksLikeDate || soundsLikeDate) {
      detectedType = 'DATE';
    } 
    else if (looksLikeCurrency || soundsLikeCurrency) {
      detectedType = 'NUMERIC';
    } 
    
    if (TYPE_OVERRIDES[checkName]) {
      detectedType = TYPE_OVERRIDES[checkName];
      console.log(`   -> Override applied: ${colName} forced to ${detectedType}`);
    }
    
    finalSchemaFields.push({ name: colName, type: detectedType });
  }
  
  console.log(`[SCHEMA] Schema successfully built!`);
  return { schema: finalSchemaFields, delimiter: fileDelimiter };
}

// ============================================================================
// 3. PROCESS SINGLE FILE WITH VERBOSE LOGGING AND SQL CASCADE
// ============================================================================
function processSingleBQFile(fileObj) {
  const archiveFolder = DriveApp.getFolderById(ARCHIVE_FOLDER_ID);
  const file = DriveApp.getFileById(fileObj.id);
  const lowerName = fileObj.name.toLowerCase();
  
  console.log(`\n======================================================`);
  console.log(`[SERVER] STARTING IMPORT PIPELINE: ${fileObj.name}`);
  console.log(`======================================================`);
  
  let headerRow = 1; 
  let dataRow = 2;
  let forcedDelimiter = null; 
  
  for (let i = 0; i < FILE_RULES.length; i++) {
    if (lowerName.includes(FILE_RULES[i].keyword)) {
      headerRow = FILE_RULES[i].headerRow;
      dataRow = FILE_RULES[i].dataRow;
      forcedDelimiter = FILE_RULES[i].delimiter || null;
      console.log(`[SERVER] Match found! Rule: ${FILE_RULES[i].keyword}. Header: ${headerRow}, Data: ${dataRow}`);
      break;
    }
  }

  let tableName = cleanTableName(fileObj.name);
  let tempTableId = tableName + '_temp_ext'; 
  console.log(`[SERVER] Target Table: ${tableName}`);
  
  try {
    console.log(`[SERVER] Cleaning up old temp tables...`);
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch (e) { }

    let schemaData = buildDynamicSchema(fileObj.id, headerRow, dataRow, forcedDelimiter, GCP_PROJECT_ID, DATASET_ID, fileObj.mimeType);
    let finalSchema = schemaData.schema;
    let fileDelimiter = schemaData.delimiter;

    console.log(`[SERVER] Creating Ghost Table...`);
    let ghostSchemaFields = finalSchema.map(f => ({ name: f.name, type: 'STRING' }));
    let isSheet = fileObj.mimeType === MimeType.GOOGLE_SHEETS;

    // --- UPDATED: Tell BigQuery whether it is looking at a CSV or a Google Sheet ---
    let externalDataConfiguration = {
      sourceUris: isSheet ? [`https://docs.google.com/spreadsheets/d/${fileObj.id}`] : [`https://drive.google.com/open?id=${fileObj.id}`],
      sourceFormat: isSheet ? "GOOGLE_SHEETS" : "CSV",
      autodetect: false
    };

    if (isSheet) {
      externalDataConfiguration.googleSheetsOptions = { skipLeadingRows: dataRow - 1 };
    } else {
      externalDataConfiguration.csvOptions = { skipLeadingRows: dataRow - 1, allowQuotedNewlines: true, fieldDelimiter: fileDelimiter };
    }

    let tableResource = {
      tableReference: { projectId: GCP_PROJECT_ID, datasetId: DATASET_ID, tableId: tempTableId },
      schema: { fields: ghostSchemaFields }, 
      externalDataConfiguration: externalDataConfiguration
    };
    
    BigQuery.Tables.insert(tableResource, GCP_PROJECT_ID, DATASET_ID);

    console.log(`[SERVER] Compiling dynamic SAFE_CAST SQL...`);
    let selectCols = finalSchema.map(f => {
      let colName = `\`${f.name}\``;
      let cleanStr = `CASE WHEN LOWER(TRIM(${colName})) IN ('', 'null', '-') THEN NULL ELSE TRIM(${colName}) END`;
      let sqlType = f.type.toUpperCase();

      if (sqlType === 'NUMERIC') {
        let noCurrency = `REGEXP_REPLACE(${cleanStr}, r'[^0-9,.-]', '')`;
        if (fileDelimiter === ';') {
          return `SAFE_CAST(REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '.') AS NUMERIC) AS ${colName}`;
        } else {
          return `SAFE_CAST(REPLACE(${noCurrency}, ',', '') AS NUMERIC) AS ${colName}`;
        }
      } 
      else if (sqlType === 'INT64') {
        let noCurrency = `REGEXP_REPLACE(${cleanStr}, r'[^0-9,.-]', '')`;
        if (fileDelimiter === ';') {
          return `SAFE_CAST(REPLACE(REPLACE(${noCurrency}, '.', ''), ',', '') AS INT64) AS ${colName}`;
        } else {
          return `CAST(SAFE_CAST(REPLACE(${noCurrency}, ',', '') AS NUMERIC) AS INT64) AS ${colName}`;
        }
      } 
      else if (sqlType === 'DATE') {
        return `
          COALESCE(
            SAFE_CAST(SUBSTR(${cleanStr}, 1, 10) AS DATE),
            SAFE.PARSE_DATE('%d.%m.%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{4}')),
            SAFE.PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT(${cleanStr}, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'))
          ) AS ${colName}
        `.trim();
      } 
      else {
        return `${cleanStr} AS ${colName}`;
      }
    }).join(',\n          ');

    let insertHeaders = finalSchema.map(f => `\`${f.name}\``).join(', ');

    let query = `
      CREATE OR REPLACE TABLE \`${GCP_PROJECT_ID}.${DATASET_ID}.${tableName}\` (
        ${finalSchema.map(f => `\`${f.name}\` ${f.type}`).join(', ')}
      );
      
      INSERT INTO \`${GCP_PROJECT_ID}.${DATASET_ID}.${tableName}\` (${insertHeaders})
      SELECT ${selectCols} FROM \`${GCP_PROJECT_ID}.${DATASET_ID}.${tempTableId}\`;
    `;

    console.log(`[SERVER] Sending SQL job to BigQuery...`);
    let queryJobConfig = { configuration: { query: { query: query, useLegacySql: false } } };
    let insertedJob = BigQuery.Jobs.insert(queryJobConfig, GCP_PROJECT_ID);
    let jobId = insertedJob.jobReference.jobId;
    let jobLocation = insertedJob.jobReference.location; 
    
    console.log(`[SERVER] Job ID ${jobId} successfully submitted. Beginning polling loop...`);
    
    let maxAttempts = 150; 
    let success = false;
    let errorMsg = "";

    for (let i = 0; i < maxAttempts; i++) {
      try {
        let job = BigQuery.Jobs.get(GCP_PROJECT_ID, jobId, { location: jobLocation });
        console.log(`   -> Polling [${i+1}/${maxAttempts}]: State is ${job.status.state}`);
        
        if (job.status.state === 'DONE') {
          if (job.status.errorResult) errorMsg = job.status.errorResult.message;
          else success = true;
          break;
        }
      } catch (pollError) {
        console.log(`   -> Minor API hiccup (${pollError.message}). Retrying...`);
      }
      Utilities.sleep(2000); 
    }

    console.log(`[SERVER] Cleaning up Ghost Table...`);
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch(e) {}
    
    if (success) {
      console.log(`[SUCCESS] BigQuery import successful. Moving file to Archive...`);
      file.moveTo(archiveFolder);
      return { success: true, log: `[SUCCESS] Injected into '${tableName}'. Moved to Archive.` };
    } else if (errorMsg) {
      console.error(`[CRASH] BigQuery rejected the file: ${errorMsg}`);
      return { success: false, log: `[ERROR] BigQuery rejected ${fileObj.name}: ${errorMsg}` };
    } else {
      console.error(`[CRASH] Polling timed out after 5 minutes.`);
      return { success: false, log: `[ERROR] Timeout waiting for database.` };
    }
    
  } catch (error) {
    console.error(`[CRASH] Critical Pipeline Error: ${error.message}`);
    try { BigQuery.Tables.remove(GCP_PROJECT_ID, DATASET_ID, tempTableId); } catch(e){}
    return { success: false, log: `[CRITICAL] Connection failed: ${error.message}` };
  }
}