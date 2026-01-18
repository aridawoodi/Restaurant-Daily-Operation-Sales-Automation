const CONFIG = {
  spreadsheetId: "1JgTZ70ZjdpaYqKtXZ4Qmf_8H57wqXjsWrWwGQxWbj2s",
  mainFolderId: "1fr63Bo6L7RBa3ID_shGWFM88SZI6xuPM",
  salesFolderId: "1JLIW_mG0xR-Ny0onSNt_QLTAuPsT7wDJ",
  laborFolderId: "1JNHcL1SWNtp7ypQXsUWtCGp53z97UHzD",
  salesFolderPrefix: "SalesSummary_",
  salesFolderPattern: /^SalesSummary_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}$/i,
  payrollFilePattern: /^PayrollExport_\d{4}_\d{2}_\d{2}-\d{4}_\d{2}_\d{2}\.csv$/i,
  salesCsvToTab: {
    "Payments summary.csv": "Sales_Payments",
    "Revenue summary.csv": "Sales_Revenue",
    "Sales category summary.csv": "Sales_Category",
    "Service Daypart summary.csv": "Sales_Daypart"
  },
  laborTabName: "Labor_Input",
  classificationSheetName: "Job_Classification_Lookup",
  logSheetName: "AppScript_Log",
  weekEndingHeaderNames: ["week ending date", "week_ending_date"],
  classificationHeaderNames: ["clasification", "classification"],
  jobTitleHeaderNames: ["job title", "job_title"],
  processOldest: false
};
