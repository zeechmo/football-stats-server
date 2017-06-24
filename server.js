const express = require('express')
var mysql = require('mysql')
var path = require('path')
var config = require('./config');
var cors = require('cors')
var _ = require('underscore');

const app = express()

app.use(cors())

app.get('/getstats', function (request, response) {
	
	var connection = mysql.createConnection({
	  host: config.mysql.hostname,
	  user: config.mysql.user,
	  password: config.mysql.password,
	  database: config.mysql.database
	})

	connection.connect(function(err) {
	  if (err) throw err
	  console.log('You are now connected...')
	  
	  // TODO: let the year be passed into the request
	  let year = 2016;
	  
	  let schoolSql = "select distinct refName from schedules where year = " + year + " order by refName;";
	  
	  connection.query(schoolSql, function(error, schools) {
		if (error) throw error
		
			let sql = "select a.* FROM adjusted a LEFT JOIN adjusted b on a.schoolRefName = b.schoolRefName and a.date < b.date WHERE b.date IS NULL  and a.year = " + year + " ORDER BY a.schoolRefName;"
	  
			connection.query(sql, function(err, results) {
				if (err) throw err
			
				console.log('here');
				
				// build an identity matrix to store school stats
				let schoolStats = {};
				_.each(schools, function(school) {
					schoolStats[school.refName] = {"schoolRefName": school.refName};
				});	

				console.log(schoolStats);
				
				// remove RowDataPacket by transforming to a json string and back into pure json
				results = JSON.parse(JSON.stringify(results));
				console.log(results);
				for (var i = 0; i < results.length; i++) {
				    schoolStats[results[i].schoolRefName][results[i].statName] = results[i].statValue;
				}
				
				console.log(schoolStats);
				
				// transform schoolStats into an array
				let toReturn = [], item;
				for (var school in schoolStats) {
					item = {};
					item.schoolRefName = school;
					item.passYards = schoolStats[school].passYards;
					item.rushYards = schoolStats[school].rushYards;
					item.passYardsAllowed = schoolStats[school].passYardsAllowed;
					item.rushYardsAllowed = schoolStats[school].rushYardsAllowed;
					toReturn.push(item);
				}

				response.json({items: toReturn});
		  })		
      })
	})
})



app.get('*', function(request, response) {
	// load single page app html
	//response.sendFile(path.join(__dirname, './public', 'index.html'));
	response.send('Are you ready for some football?');
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
})
