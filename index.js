const TIMESTAMP_COLUMN_NAME = "Timestamp (UTC+07:00)";
const FILE_NAME = "discharge1";
const DATA_FOLDER_NAME = "/data-source";
// ----------------------------------
const csv = require("csv-parser");
const fs = require("fs");

const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// ----------------------------------

class AggregateContainer {
  sumMonths(accumulator, current) {
    return Number(accumulator) + Number(current);
  }

  calculateAverage() {
    for (const year in this) {
      for (const month in this[year]) {
        this[year][month].average =
          this[year][month].reduce(this.sumMonths) / this[year][month].length;
      }
    }
  }
}

const writeFile = (container, unit, parameter, fileName) => {
  const csvWriter = createCsvWriter({
    path: "output-" + fileName,
    header: [
      { id: "year", title: "Year" },
      { id: "month", title: "Month" },
      { id: "average", title: "Average" },
      { id: "unit", title: "Unit" },
      { id: "parameter", title: "Parameter" },
    ],
  });

  console.log("writing");
  let data = [];

  for (const year in container) {
    for (const month in container[year]) {
      data.push({
        year,
        month,
        average: container[year][month].average,
        unit,
        parameter,
      });
    }
  }

  csvWriter
    .writeRecords(data)
    .then(() => {
      console.log("Calculations complete! Please check the output file!");
    })
    .catch((err) => {
      console.log(err);
    });
};

const main = (fileName, container) => {
  console.log("running main");
  let unit, parameter;

  fs.createReadStream(`./data-source/${fileName}`)
    .pipe(csv())
    .on("data", (row) => {
      if (!parameter || !unit) {
        parameter = row.Parameter;
        unit = row.Unit;
        console.log("Assigned parameter and unit");
      }
      const [year, month] = row[TIMESTAMP_COLUMN_NAME].split("-");
      if (container[year] && container[year][month]) {
        // both year and month exist
        container[year][month].push(row.Value);
      } else if (container[year] && !container[year][month]) {
        // month doesn't exist
        container[year][month] = [row.Value];
      } else {
        // both year and month don't exist
        container[year] = { [month]: [row.Value] };
      }
    })
    .on("end", () => {
      container.calculateAverage();
      writeFile(container, unit, parameter, fileName);
    });
};

if (!fs.existsSync("./data-source")) {
  fs.mkdirSync("./data-source");
  return console.log(
    "Source directory not found. Creating it now! Please put all your CSV files inside the /data-source folder."
  );
}

const files = fs.readdirSync("./data-source");

if (files.length > 0) {
  for (const file of files) {
    const container = new AggregateContainer();

    main(file, container);
  }
} else {
  console.log("There are no files inside the data-source folder.");
}
