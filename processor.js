const AWS = require("aws-sdk");
const { PrismaClient } = require("@prisma/client");
const xlsx = require("xlsx");
const prisma = new PrismaClient();

// For monitoring

// AWS S3 configuration
const s3 = new AWS.S3();
const BUCKET_NAME = "projecty-test-data"; // Replace with your bucket name

function parseExcelDate(excelDate) {
  // Excel stores dates as a serial number, so we convert it into a JavaScript Date object.
  const date = new Date((excelDate - 25569) * 86400 * 1000);
  return date;
}

// Helper function to fetch and parse Excel files from S3
async function fetchExcelFile(key, fileCounter) {
  const params = { Bucket: BUCKET_NAME, Key: key };
  const data = await s3.getObject(params).promise();
  const workbook = xlsx.read(data.Body, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const xlsData = xlsx.utils.sheet_to_json(sheet, { header: 1 });

  xlsData.forEach((row, _) => {
    row.forEach((cell, colIndex) => {
      if (colIndex === 3 || colIndex === 4) {
        row[colIndex] = parseExcelDate(cell); // convert Excel date to JavaScript Date
      }
    });
  });

  console.log(`Info: Got the excel sheet from s3 for ${fileCounter}`);

  return xlsData;
}

// This will run once and get all the s3 items for that bucket
async function listAllObjects(bucketName) {
  let isTruncated = true;
  let objects = [];
  let continuationToken = null;

  while (isTruncated) {
    const params = {
      Bucket: bucketName,
      ContinuationToken: continuationToken, // Pagination token
    };

    const data = await s3.listObjectsV2(params).promise();
    objects = objects.concat(data.Contents);

    isTruncated = data.IsTruncated; // Check if there are more items
    continuationToken = data.NextContinuationToken; // Get the next token
  }

  return objects;
}

// Main function
async function processAndInsertData(imeiList, startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);

  try {
    // List all files in the bucket
    const Contents = await listAllObjects(BUCKET_NAME);

    // Filter files by date range and IMEI numbers
    const filteredFiles = Contents.filter(({ Key }) => {
      // Split the key to separate the directory and file name
      const [imeiDir, fileName] = Key.split("/");
      const imei = imeiDir.split("-")[1]; // Extract IMEI from "IMEI-350317177724063"

      // Extract the full date (up to the time) from the file name
      const dateMatch = fileName.match(/^(\d{4}-\d{2}-\d{2})/);
      const fileDate = dateMatch ? dateMatch[1] : null;

      const formatedFileDate = new Date(fileDate);

      if (!fileDate) {
        console.warn(`Error: Invalid date format in file: ${Key}`);
        return false;
      }

      // Filter by IMEI and date range

      return (
        imeiList.includes(parseInt(imei)) &&
        formatedFileDate >= start &&
        formatedFileDate <= end
      );
    });

    console.log(`Info: Processing total of ${filteredFiles.length} files\n\n`);

    let completedFileCounter = 1;
    for (const file of filteredFiles) {
      console.time(`Processing time for file ${completedFileCounter}`);
      try {
        console.log(
          `Info: Currently processing file number ${completedFileCounter} with key: ${file.Key}`
        );

        // Fetch and parse the file
        const data = await fetchExcelFile(file.Key, completedFileCounter);

        // Map and insert data into the Prisma database

        const headers = [
          "_id",
          "CustomerCode",
          "IMEI",
          "Timestamp",
          "Actual",
          "Longitude",
          "Latitude",
          "Altitude",
          "Angle",
          "Speed",
        ];

        const records = data.slice(1).map((row) => {
          const rowData = {};
          headers.forEach((header, index) => {
            rowData[header] = row[index]; // Map the header to the corresponding value in the row
          });

          return {
            Actual: new Date(rowData["Actual"]),
            Altitude: parseFloat(rowData["Altitude"]),
            Angle: parseFloat(rowData["Angle"]),
            IMEI: rowData["IMEI"],
            Longitude: parseFloat(rowData["Longitude"]),
            Speed: parseFloat(rowData["Speed"]),
            Timestamp: new Date(rowData["Timestamp"]),
            Latitude: parseFloat(rowData["Latitude"]),
            CustomerCode: parseInt(rowData["CustomerCode"], 10),
          };
        });

        if (records.length > 0) {
          try {
            // Bulk insert using createMany
            await prisma.latestData.createMany({
              data: records,
              skipDuplicates: true, // Ensures duplicates are ignored
            });
            console.log(
              `Info: Inserted ${records.length} records successfully.`
            );
          } catch (err) {
            console.error("Error: Failed to insert records in bulk:", err);
          }
        }

        console.timeEnd(`Processing time for file ${completedFileCounter}`);

        console.log(
          `\n\nInfo: ${Math.round(
            (completedFileCounter / filteredFiles.length) * 100
          )}% of total files are completed \n \n`
        );
        completedFileCounter++;
      } catch (error) {
        console.log(`Error: There was some error with ${file.Key}`);
      }
    }

    console.log("Info: Data processing and insertion completed.");
  } catch (err) {
    console.error("Error: Error processing files:", err);
  } finally {
    await prisma.$disconnect();
  }
}

module.exports = processAndInsertData;
