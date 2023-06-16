let fs = require('fs');
let csv = require('csv-parser');
let axios = require('axios');
let createCsvWriter = require('csv-writer').createObjectCsvWriter;
let async = require('async');

let csvWriter = createCsvWriter({
    path: 'output.csv',
    header: [
        {id: 'account', title: 'ACCOUNT'},
        {id: 'lat', title: 'LATITUDE'},
        {id: 'lng', title: 'LONGITUDE'},
        {id: 'address', title: 'ADDRESS'},
    ]
});

let data = [];
let totalRequests = 0;

fs.createReadStream('energygps.csv')
    .pipe(csv())
    .on('data', (row) => {
        data.push(row);
    })
    .on('end', () => {
        totalRequests = data.length;

        // limit the number of simultaneous requests
        let limit = 1;
        
        async.eachLimit(data, limit, (row, callback) => {
            let lat = row.lat;
            let lng = row.lng;
            
            axios.get(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}`, {
                headers: {
                    'User-Agent': 'HensallData' 
                }
            })
                .then(response => {
                    let address = response.data.display_name;
                    row.address = address;
                    let percentageComplete = (data.indexOf(row) / totalRequests) * 100;
                    console.log(`Response successful for lat ${lat} and lng ${lng}. Completed ${percentageComplete.toFixed(2)}%`);
                    setTimeout(callback, 1000); // wait 1 second between requests
                })
                .catch(error => {
                    console.log(error);
                    setTimeout(callback, 1000); // wait 1 second even if there was an error
                });
        }, (err) => {
            if (err) console.error(err);
            
            csvWriter
                .writeRecords(data)
                .then(()=> console.log('The CSV file was written successfully'));
        });
    });
