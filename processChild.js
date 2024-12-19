const fs = require('fs');
const path = require('path');
const processAndInsertData = require('./processor');

// Get the parameters passed from the parent process
const imeiList = JSON.parse(process.argv[2]); // The subset of IMEI numbers
const startDate = process.argv[3];
const endDate = process.argv[4];
const logFileName = process.argv[5];  // Log file name passed by the parent process

// Log file path (dynamic based on the child index)
const logFilePath = path.join(__dirname, logFileName);

// Function to append logs to the file
const logToFile = (message) => {
  fs.appendFileSync(logFilePath, message + '\n', 'utf8');
};

// Redirect `console.log` and `console.error` to the log file
console.log = (message) => logToFile(message);
console.error = (message) => logToFile(message);

// Log the starting message
logToFile(`Started processing IMEIs: ${imeiList.join(', ')} from ${startDate} to ${endDate}`);

try {
  // Call the processAndInsertData function
  processAndInsertData(imeiList, startDate, endDate);
  logToFile(`Successfully processed IMEIs: ${imeiList.join(', ')}`);
} catch (error) {
  logToFile(`Error processing IMEIs: ${imeiList.join(', ')}. Error: ${error.message}`);
}
