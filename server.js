const express = require('express')
var mysql = require('mysql')
var path = require('path')
var config = require('./config');

const app = express()

app.get('/getStats', function (request, response) {
	
	var connection = mysql.createConnection({
	  host: config.mysql.hostname,
	  user: config.mysql.user,
	  password: config.mysql.password,
	  database: config.mysql.database
	})

	connection.connect(function(err) {
	  if (err) throw err
	  console.log('You are now connected...')
	  
	  connection.query('SELECT * FROM adjusted LIMIT 1', function(err, results) {
        if (err) throw err
		var toReturn = "";
        toReturn += results[0].id + ",";
        toReturn += results[0].schoolRefName + ",";
        toReturn += results[0].statName + ",";
        toReturn += results[0].statValue;
		response.send(toReturn);
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
