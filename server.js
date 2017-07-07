const express = require('express')
var mysql = require('mysql')
var path = require('path')
var config = require('./config');
var cors = require('cors')
var _ = require('underscore');
var Q = require('Q');

const app = express()

app.use(cors())

app.get('/getstats', function(request, response) {
	
	let year = 2016;
	if (!!request.query.year) {
		year = parseInt(request.query.year, 10);
		console.log(year);
	}
		
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
			
			function doGetSchedulesAndWins() {
				var deferred = Q.defer();
				connection.query("select count(1) as wins, refName from schedules where year = " + year + " and pointsFor > pointsAgainst group by refName;",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			function doGetLosses() {
				var deferred = Q.defer();
				connection.query("select count(1) as losses, refName from schedules where year = " + year + " and pointsFor < pointsAgainst group by refName;",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			function doGetSchoolInfo() {
				var deferred = Q.defer();
				connection.query("select distinct s.refName, sc.displayName from schedules s inner join schools sc on s.refName = sc.refName where year = " + year + " order by refName;",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			function doGetOffensiveStats() {
				var deferred = Q.defer();
				connection.query("(select schoolRefName, 'pass' as stat, SUM(passYards) yards, SUM(passAttempts) attempts, COUNT(distinct s.gameId) games from passing p inner join schedules s on p.gameId = s.gameId where year = " + year + " and s.refName = p.schoolRefName group by schoolRefName) union all (select schoolRefName, 'rush' as stat, SUM(rushYards) yards, SUM(rushAttempts) attempts, COUNT(distinct s2.gameId) games from rushingreceiving r inner join schedules s2 on r.gameId = s2.gameId where year = " + year + " and s2.refName = r.schoolRefName group by schoolRefName);",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			function doGetDefensiveStats() {
				var deferred = Q.defer();
				connection.query("(select opponentRefName as schoolRefName, 'pass' as stat, SUM(passYards) yards, SUM(passAttempts) attempts, COUNT(distinct s.gameId) games from passing p inner join schedules s on p.gameId = s.gameId where year = " + year + " and s.refName = p.schoolRefName group by opponentRefName) union all (select opponentRefName as schoolRefName, 'rush' as stat, SUM(rushYards) yards, SUM(rushAttempts) attempts, COUNT(distinct s2.gameId) games from rushingreceiving r inner join schedules s2 on r.gameId = s2.gameId where year = " + year + " and s2.refName = r.schoolRefName group by opponentRefName);",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			function doGetAdjustedStats() {
				var deferred = Q.defer();
				connection.query("select * FROM adjusted WHERE (schoolRefName, year,statName, date) IN ( SELECT schoolRefName, year, statName, MAX(date) FROM adjusted where year = " + year + " group by schoolRefName, year, statName);",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			function doGetPoints() {
				var deferred = Q.defer();
				connection.query("select avg(pointsFor) as pointsFor, avg(pointsAgainst) as pointsAgainst, refName from schedules where year = " + year + " group by refName;",deferred.makeNodeResolver());
				return deferred.promise;
			}
			
			Q.all([doGetSchedulesAndWins(),
					doGetLosses(),
					doGetSchoolInfo(),
					doGetOffensiveStats(),
					doGetDefensiveStats(),
					doGetAdjustedStats(),
					doGetPoints()]).then(function(results){
				
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
							games: rawStat.games
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
							games: rawStat.games
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
					
						// meta properties
						item.schoolRefName = school
					
						// teamName
						item.teamName = schoolStats[school].displayName;
						//TODO: wins
						item.wins = teamWinLoss[school].wins;
						//TODO: losses
						item.losses = teamWinLoss[school].losses;
						//predictedPPG
						item.predictedPPG = (pointsScored[school].pointsFor).toFixed(2);
						//predictedPPGAllowed
						item.predictedPPGAllowed = (pointsScored[school].pointsAgainst).toFixed(2);
						//adjYardsPPDiff
						item.adjYardsPPDiff = (schoolStats[school].passYards - schoolStats[school].passYardsAllowed).toFixed(2);
						//adjYardsPPOff
						item.adjYardsPPOff = (schoolStats[school].passYards).toFixed(2);
						//adjYardsPPDef
						item.adjYardsPPDef = (schoolStats[school].passYardsAllowed).toFixed(2);
						//rawYardsPPDiff
						item.rawYardsPPDiff = rawYardsPerPlayDifferential;
						//rawYardsPPOff
						item.rawYardsPPOff = rawOffensiveYardsPerPlay;
						//rawYardsPPDef
						item.rawYardsPPDef = rawDefensiveYardsPerPlay;
						//playsPerGame
						item.playsPerGame = totalPlaysPerGame.toFixed(2);
					}
					catch (err) {
						console.log(err);
					}
					toReturn.push(item);
				}

				response.json({items: toReturn});
			});
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
