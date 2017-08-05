const express = require('express')
var mysql = require('mysql')
var path = require('path')
var config = require('./config');
var cors = require('cors')
var _ = require('underscore');
var Q = require('Q');

const app = express()

app.use(cors())

//
// db queries
//
function doGetSchedulesAndWins(conn, year) {
	var deferred = Q.defer();
	conn.query("select count(1) as wins, refName from schedules where year = " + year + " and pointsFor > pointsAgainst group by refName;",deferred.makeNodeResolver());
	return deferred.promise;
}

function doGetLosses(conn, year) {
	var deferred = Q.defer();
	conn.query("select count(1) as losses, refName from schedules where year = " + year + " and pointsFor < pointsAgainst group by refName;",deferred.makeNodeResolver());
	return deferred.promise;
}

function doGetSchoolInfo(conn, year) {
	var deferred = Q.defer();
	conn.query("select distinct s.refName, sc.displayName from schedules s inner join schools sc on s.refName = sc.refName where year = " + year + " order by refName;",deferred.makeNodeResolver());
	return deferred.promise;
}

function doGetOffensiveStats(conn, year) {
	var deferred = Q.defer();
	conn.query("(select schoolRefName, 'pass' as stat, SUM(passYards) yards, SUM(passAttempts) attempts, SUM(passCompletions) completions, COUNT(distinct s.gameId) games from passing p inner join schedules s on p.gameId = s.gameId where year = " + year + " and s.refName = p.schoolRefName group by schoolRefName) union all (select schoolRefName, 'rush' as stat, SUM(rushYards) yards, SUM(rushAttempts) attempts, 1 as completions, COUNT(distinct s2.gameId) games from rushingreceiving r inner join schedules s2 on r.gameId = s2.gameId where year = " + year + " and s2.refName = r.schoolRefName group by schoolRefName);",deferred.makeNodeResolver());
	return deferred.promise;
}

function doGetDefensiveStats(conn, year) {
	var deferred = Q.defer();
	conn.query("(select opponentRefName as schoolRefName, 'pass' as stat, SUM(passYards) yards, SUM(passAttempts) attempts, SUM(passCompletions) completions, COUNT(distinct s.gameId) games from passing p inner join schedules s on p.gameId = s.gameId where year = " + year + " and s.refName = p.schoolRefName group by opponentRefName) union all (select opponentRefName as schoolRefName, 'rush' as stat, SUM(rushYards) yards, SUM(rushAttempts) attempts, 1 as completions, COUNT(distinct s2.gameId) games from rushingreceiving r inner join schedules s2 on r.gameId = s2.gameId where year = " + year + " and s2.refName = r.schoolRefName group by opponentRefName);",deferred.makeNodeResolver());
	return deferred.promise;
}

function doGetAdjustedStats(conn, year) {
	var deferred = Q.defer();
	conn.query("select * FROM adjusted WHERE (schoolRefName, year,statName, date) IN ( SELECT schoolRefName, year, statName, MAX(date) FROM adjusted where year = " + year + " group by schoolRefName, year, statName);",deferred.makeNodeResolver());
	return deferred.promise;
}

function doGetPoints(conn, year) {
	var deferred = Q.defer();
	conn.query("select avg(pointsFor) as pointsFor, avg(pointsAgainst) as pointsAgainst, refName from schedules where year = " + year + " group by refName;",deferred.makeNodeResolver());
	return deferred.promise;
}

function getAllStats(year, callback) {
			
	var connection = mysql.createConnection({
		host: config.mysql.hostname,
		user: config.mysql.user,
		password: config.mysql.password,
		database: config.mysql.database
	})
	connection.connect(function(err) {
		if (err) {
			throw err
		}
		console.log('You are now connected...')
		
		var d = require('domain').create()
			d.on('error', function(err){
			// handle the error safely
			console.log(err)
		})
		
		d.run(function() {
			
			Q.all([doGetSchedulesAndWins(connection, year),
					doGetLosses(connection, year),
					doGetSchoolInfo(connection, year),
					doGetOffensiveStats(connection, year),
					doGetDefensiveStats(connection, year),
					doGetAdjustedStats(connection, year),
					doGetPoints(connection, year)]).then(function(results){
				
				let wins = results[0][0];
				let losses = results[1][0];
				let schools = results[2][0];
				let offense = results[3][0];
				let defense = results[4][0];
				let adjusted = results[5][0];
				let points = results[6][0];
				
				// calculate win/loss record for each team
				let teamWinLoss = {};
				_.each(wins, function(win) {
					teamWinLoss[win.refName] = {
						"wins": win.wins
					}
				});
				_.each(losses, function(loss) {
					teamWinLoss[loss.refName].losses = loss.losses;
				});
				
				let pointsScored = {};
				_.each(points, function(p) {
					pointsScored[p.refName] = {
						pointsFor: p.pointsFor,
						pointsAgainst: p.pointsAgainst
					};
				});
				
				let rawOffensiveStats = {};
				_.each(offense, function(rawStat) {
					if (!rawOffensiveStats[rawStat.schoolRefName]) {
						rawOffensiveStats[rawStat.schoolRefName] = {};
					}
					
					if (rawStat.stat === "pass") {
						rawOffensiveStats[rawStat.schoolRefName] = {
							passYards: rawStat.yards,
							passAttempts: rawStat.attempts,
							games: rawStat.games,
							completions: rawStat.completions
						};
					}
					else if (rawStat.stat === "rush") {
						rawOffensiveStats[rawStat.schoolRefName]['rushYards'] = rawStat.yards;
						rawOffensiveStats[rawStat.schoolRefName]['rushAttempts'] = rawStat.attempts;
					}
				});
				
				let rawDefensiveStats = {};
				_.each(defense, function(rawStat) {
					if (!rawDefensiveStats[rawStat.schoolRefName]) {
						rawDefensiveStats[rawStat.schoolRefName] = {};
					}
					
					if (rawStat.stat === "pass") {
						rawDefensiveStats[rawStat.schoolRefName] = {
							passYards: rawStat.yards,
							passAttempts: rawStat.attempts,
							games: rawStat.games,
							completions: rawStat.completions
						};
					}
					else if (rawStat.stat === "rush") {
						rawDefensiveStats[rawStat.schoolRefName]['rushYards'] = rawStat.yards;
						rawDefensiveStats[rawStat.schoolRefName]['rushAttempts'] = rawStat.attempts;
					}
				});
				
				// build an identity matrix to store school stats
				let schoolStats = {};
				_.each(schools, function(school) {
					schoolStats[school.refName] = {
						schoolRefName: school.refName,
						displayName: school.displayName
					};
				});

				// remove 'RowDataPacket' by transforming to a json string and back into pure json
				adjusted = JSON.parse(JSON.stringify(adjusted));
				for (var i = 0; i < adjusted.length; i++) {
					schoolStats[adjusted[i].schoolRefName][adjusted[i].statName] = adjusted[i].statValue;
				}
			
				// transform schoolStats into an array
				let toReturn = [], item;
				for (var school in schoolStats) {
				
					try {
						item = {};
						
						// calculations
						let rawOffensiveYardsPerPlay = ((rawOffensiveStats[school].rushYards + rawOffensiveStats[school].passYards) / (rawOffensiveStats[school].rushAttempts + rawOffensiveStats[school].passAttempts)).toFixed(2);
						let rawDefensiveYardsPerPlay = ((rawDefensiveStats[school].rushYards + rawDefensiveStats[school].passYards) / (rawDefensiveStats[school].rushAttempts + rawDefensiveStats[school].passAttempts)).toFixed(2);
						let rawYardsPerPlayDifferential = (rawOffensiveYardsPerPlay - rawDefensiveYardsPerPlay).toFixed(2);
						let totalOffensivePlays = rawOffensiveStats[school].rushAttempts + rawOffensiveStats[school].passAttempts;
						let totalDefensivePlays = rawDefensiveStats[school].rushAttempts + rawDefensiveStats[school].passAttempts;
						let totalPlaysPerGame = (totalOffensivePlays + totalDefensivePlays) / (teamWinLoss[school].wins + teamWinLoss[school].losses);
						let totalGames = (teamWinLoss[school].wins + teamWinLoss[school].losses);
					
						// meta properties
						item.schoolRefName = school
					
						// teamName
						item.teamName = schoolStats[school].displayName;
						item.wins = teamWinLoss[school].wins;
						item.losses = teamWinLoss[school].losses;
						item.predictedPPG = (pointsScored[school].pointsFor).toFixed(2);
						item.predictedPPGAllowed = (pointsScored[school].pointsAgainst).toFixed(2);
						item.adjYardsPPDiff = (schoolStats[school].passYards - schoolStats[school].passYardsAllowed).toFixed(2);
						item.adjYardsPPOff = ((schoolStats[school].passYards * rawOffensiveStats[school].passAttempts / totalOffensivePlays) + (schoolStats[school].rushYards * rawOffensiveStats[school].rushAttempts / totalOffensivePlays)).toFixed(2);
						item.adjYardsPPDef = ((schoolStats[school].passYardsAllowed * rawDefensiveStats[school].passAttempts / totalDefensivePlays) + (schoolStats[school].rushYardsAllowed * rawDefensiveStats[school].rushAttempts / totalDefensivePlays)).toFixed(2);
						item.rawYardsPPDiff = rawYardsPerPlayDifferential;
						item.rawYardsPPOff = rawOffensiveYardsPerPlay;
						item.rawYardsPPDef = rawDefensiveYardsPerPlay;
						item.playsPerGame = totalPlaysPerGame.toFixed(2);
						item.pointsPerGame = (pointsScored[school].pointsFor).toFixed(2);
						item.adjPassYards = (schoolStats[school].passYards).toFixed(2);
						item.passYards = ((rawOffensiveStats[school].passYards) / (rawOffensiveStats[school].passAttempts)).toFixed(2);
						item.passAttemptsPerGame = (rawOffensiveStats[school].passAttempts / totalGames).toFixed(2);
						item.passCompletePct = (rawOffensiveStats[school].completions * 100 / rawOffensiveStats[school].passAttempts).toFixed(2);
						item.adjRushYards = (schoolStats[school].rushYards).toFixed(2);
						item.rushYards = ((rawOffensiveStats[school].rushYards) / (rawOffensiveStats[school].rushAttempts)).toFixed(2);
						item.rushAttemptsPerGame = (rawOffensiveStats[school].rushAttempts / totalGames).toFixed(2);
						
						item.pointsPerGameAllowed = (pointsScored[school].pointsAgainst).toFixed(2);
						item.adjPassYardsAllowed = (schoolStats[school].passYardsAllowed).toFixed(2);
						item.passYardsAllowed = ((rawDefensiveStats[school].passYards) / (rawDefensiveStats[school].passAttempts)).toFixed(2);
						item.passAttemptsPerGameAllowed = (rawDefensiveStats[school].passAttempts / totalGames).toFixed(2);
						item.passCompletePctAllowed = (rawDefensiveStats[school].completions * 100 / rawDefensiveStats[school].passAttempts).toFixed(2);
						item.adjRushYardsAllowed = (schoolStats[school].rushYardsAllowed).toFixed(2);
						item.rushYardsAllowed = ((rawDefensiveStats[school].rushYards) / (rawOffensiveStats[school].rushAttempts)).toFixed(2);
						item.rushAttemptsPerGameAllowed = (rawDefensiveStats[school].rushAttempts / totalGames).toFixed(2);
						
					}
					catch (err) {
						console.log(err);
					}
					toReturn.push(item);
				}

				callback(toReturn);
			});
		})
	})
}

app.get('/getoffensestats', function(request, response) {

	let year = 2016;
	if (!!request.query.year) {
		year = parseInt(request.query.year, 10);
	}
	
	getAllStats(year, function(toReturn){
		response.json({items: toReturn});
	});

})

app.get('/getstats', function(request, response) {
	
	let year = 2016;
	if (!!request.query.year) {
		year = parseInt(request.query.year, 10);
	}
	
	getAllStats(year, function(toReturn){
		response.json({items: toReturn});
	});

})

app.get('/getdefensestats', function(request, response) {

	let year = 2016;
	if (!!request.query.year) {
		year = parseInt(request.query.year, 10);
	}
	
	getAllStats(year, function(toReturn){
		response.json({items: toReturn});
	});

})

app.get('*', function(request, response) {
	// load single page app html
	//response.sendFile(path.join(__dirname, './public', 'index.html'));
	response.send('Are you ready for some football?');
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
})
