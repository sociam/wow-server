/**
 * 
 */
var amqp = require('amqplib');
var app = require('http').createServer(handler);
var io = require('socket.io')(app);
var fs = require('fs');
var _ = require('underscore');
var wpimg = require('wikipedia-image');
// var config = require('./config');

//app.timeout = 0;// trying to fix a stupid error
app.listen(9001);

var startup_date = new Date();

var active_users = 0;
var temp_user_cnt = 0;

// var keyword_filter = "";
// var language_filter = "";

var processed_msg_cnt = 0;

// don't need the global filters_made
// var filters_made = [];

// DATA STRUCTURE ds[room]= {}
	// vizs=[]
	// filter={}
		// filter.keyword
		// filter.language
	// filters_made=[]

var ds = [];
var rooms= [];

function showErr(e) {
	console.error(e, e.stack);
}

function handler(req, res) {
	res.writeHead(200);
	res.end("");
}

io.sockets.setMaxListeners(0);

// SOCKET DETAILS
// Current socketnames in use:
	// user_heartbeat
	// active_user
	// filter
	// filter_keyword
	// set_filter_keyword
	// filter_lang
	// set_filter_lang
	// processed_messages;

// Hoses (Mappings from RabbitMQ)
	// trends
	// news
	// twitter
	// spinn3r
	// wikipedia_revisions
	// wikipedia_images
	// twitter_delete
	// twitter_delete_pulse

io.on('connection', function(socket) {

	socket.on('newViz', function(data) {
		var newViz = {};
		newViz.vizName = data.vizName;
		newViz.currentDate = data.currentDate;

		console.log("");
		console.log("NewViz created");

		// if room exists
		if (ds[data.room] != undefined) {

			// push newViz in there
			ds[data.room].vizs.push(newViz);
			console.log(data.room + " already exists and " + newViz.vizName
					+ " is in here");
			// if room doesn't exist
		} else {
			ds[data.room] = {};
			//add this new room the array of rooms
			rooms.push(data.room);
			ds[data.room].vizs = [];
			ds[data.room].vizs.push(newViz);
			// add an empty filter to the newly created room
			ds[data.room].filter = {
				// set the filter to this new room to false
				"filter" : false,
				"keyword_filter" : "",
				"language_filter" : ""
			};
			//array for filters applied to this room
			ds[data.room].filters_made = [];
			console.log(data.room + " was just created and " + newViz.vizName
					+ " was added to this room now");
			//socket.emit("addNewRoom", data.room);
			
			//socket.emit("filter", filter); // emit the current state to this client
			
		}

		// finally send newViz to the specified room
		socket.join(data.room);
		//socket.broadcast.to(data.room).emit('newVizJoined', newViz.vizName);
		//show the filters in that room
		socket.emit('existing_filters', ds[data.room].filters_made);
		io.sockets.to(data.room).emit("set_filter_keyword",ds[data.room].filter.keyword_filter);
		console.log("set_filter keyword emitted");
		io.sockets.to(data.room).emit("set_filter_lang", ds[data.room].filter.language_filter);
		console.log("set_filter lang emitted");
		//emit this to the filter so that the filter checks the room and emits the right filter to the viz
		//io.sockets.to(data.room).emit('checkRoom', {"vizName": newViz.vizName, "room": data.room});
		console.log("The vizs in this room are: "+ "\n"+ ds[data.room].vizs);
		console.log("Current Date HEREEEE:" + newViz.currentDate);
	});

	// my version of 'filter'
	// should receive the filter and also the room that the filter applies to
	socket.on("filter", function(data) {
		console.log("Filter updated:", data.newFilter);
		// if room exists update filter, combine it and send to clients in the
		// specified room
		if (ds[data.room] != undefined) {
			_.extend(ds[data.room].filter, data.newFilter);
			var tempFilter=ds[data.room].filter;
			io.sockets.to(data.room).emit("filter", tempFilter.filter);
			io.sockets.to(data.room).emit("set_filter_keyword",
					tempFilter.keyword_filter);
			io.sockets.to(data.room).emit("set_filter_lang",
					tempFilter.language_filter);
		} else {
			console.log("Room doesn't exist");
		}
	});

	socket.on('get_filter_list', function(room) {
		// update with the last few items of the filter_list in that room
		try {
			var last_filters= ds[room].filters_made;
			sendLastFilterItems({"room": room,"last_filters":last_filters.slice((last_filters.length - 3),
					(last_filters.length - 1))});
			console.log("Filter list: "+ "\n"+ ds[room].filters_made);
		} catch (e) {
			// Might be an empty list...
		}
	});

	// should also receive the room along with the newFilter
	socket.on('filter_keyword',function(data) {
		console.log("Filter_keyword updated:", data.newFilter);
		if (ds[data.room] != undefined) {
			var tempFilter=ds[data.room].filter;
			tempFilter.filter = true;
			tempFilter.keyword_filter = data.newFilter;
			//emit to the sockets in the room
			io.sockets.to(data.room).emit("filter", tempFilter.filter);
			io.sockets.to(data.room).emit("set_filter_keyword", tempFilter.keyword_filter);
			
			//add the new filter to the filters_made
			addToFilterList(data.room,"keyword", tempFilter.keyword_filter);
			
		}else{
			console.log("Room doesn't exist");
		}
	});
	
	//receive a filter update, combine it and sent it to the clients in the specified ROOM!!
	socket.on('filter_lang',function(data) {
		if (ds[data.room] != undefined) {
			var tempFilter=ds[data.room].filter;
			tempFilter.filter = true;
			tempFilter.language_filter = data.newFilter;
			io.sockets.to(data.room).emit("filter", tempFilter.filter);
			io.sockets.to(data.room).emit("set_filter_lang", tempFilter.language_filter);
			
			addToFilterList(data.room, "language", tempFilter.language_filter);
			console.log("Filter_language updated:", data.newFilter);

		}else{
			console.log("Room doesn't exist");
		}
	});
	
	// new ms user...
	// receive a filter update, combine it and send to ALL clients
	socket.on('active_user', function(newFilter) {
		// console.log("filter updated:", newFilter);
		// console.log("emitting filter:", filter);
		++temp_user_cnt;
	});

});

function emit_processed_message_count() {
	if (processed_msg_cnt > 3000000) {
		processed_msg_cnt = 0;
	}
	try {
		io.emit("processed_msg_cnt", processed_msg_cnt);
	} catch (e) {
		console.log("failing here" + e)
	}
}
// reset filters every 60 seconds - just for sanity...
// var processed_msgcnt_interval =
// setInterval(function(){emit_processed_message_count()}, 2000);

// send a list of initial items
function sendLastFilterItems(data) {
	io.sockets.to(data.room).emit("existing_filters", data.last_filters);
}

// add to the current list of filters....IN THAT ROOM!!
// modified this function to also send the room as one of the parameters
function addToFilterList(room, type, filter_string) { 
	var date = new Date();
	var data = {
		"type" : type,
		"filter" : filter_string,
		"timestamp" : date
	}
	try {
		ds[room].filters_made.push(data);
	} catch (e) {
		console.log("Failing here, room might not exist");
	}
	// change this emit to know what room we're talking about
	io.sockets.to(room).emit("new_filter_item", data);
}

// reset all filters
function resetKeywords(room) {
	
	keyword_filter = "";
	language_filter = "";

	// let the clients in that room know
	io.sockets.to(room).emit("set_filter_keyword", keyword_filter);
	io.sockets.to(room).emit("set_filter_lang", language_filter);
}

// reset filters every 60 seconds - just for sanity...
var resetKeywords_interval = setInterval(function(room){resetKeywords(room)},1200000);

// Update the master count with the temp count
function updateUserCount() {
	active_users = temp_user_cnt;
}

// want to send a heartbeat to all users to see how many are still connected!
function checkForUsers() {
	// send a heartbeat
	temp_user_cnt = 0;
	io.emit("user_heartbeat", "blabla");
}

// want to send a heartbeat to all users to see how many are still connected!
function emitUserCount() {
	// send a heartbeat
	io.emit("active_user_count", active_users);
}

// These Control the User Count Details
var checkForUsers_interval = setInterval(function() {
	checkForUsers()
}, 1000);
var updateUserCount_interval = setInterval(function() {
	updateUserCount()
}, 2000);
var emit_userCount = setInterval(function() {
	emitUserCount()
}, 1000);

//check filters for a particular room
function checkFilters(msg, room) {

	var match_keyword = true;
	var match_lang = true;

	var tempFilter=ds[room].filter;
	// is the filter enabled?
	if (tempFilter.filter) {

		// check for keyword filter
		if (msg.content.toString().indexOf(tempFilter.keyword_filter) > -1) {
			match_keyword = true;
		} else {
			match_keyword = false;
		}

		if (tempFilter.language_filter.length > 0) {

			// var data = JSON.parse(msg.content.toString());
			try {
				if (msg.content.toString().indexOf('"' + tempFilter.language_filter + '"') > -1) {
					match_lang = true;
				} else {
					match_lang = false;
				}
			} catch (e) {
				console.log(e)
				match_lang = false;
			}
		}

		if (match_lang && match_keyword) {
			return true;
		} else {
			return false;
		}

	} else {

		return true;
	}

}

// here we worry about the message sending
// We perform raw filtering here!
var emitMsg = function(outName, msg) {
	try {
		// ++processed_msg_cnt;
		
		//loop through all the rooms in the ds and apply the right filters to each room
		for(var room=0;room<rooms.length;room++){
			if (checkFilters(msg, rooms[room])) {

				var data = JSON.parse(msg.content.toString());
				//emit data to that room?
				io.to(rooms[room]).emit(outName, data);

				// console.log(outName);
				// make the revisions images feed
				if (outName == "wikipedia_revisions") {
					// console.log(ring());
					var page_url = data.wikipedia_page_url;
					if (page_url) {
						wpimg(page_url).then(function(image) {
							if (image && image != "") {
								//emit to the entire room?
								io.emit('wikipedia_images', {
									"image_url" : image,
									"data" : data
								});
							}

						}, function(e) {
							// error querying etc
						});
					}
				}

				// end of filter
			}
		}
		// do a raw match on the message
		

	} catch (e) {
		//
	}
}

var connectQueue = function(queueName, outName) {
	return amqp.connect("amqp://admin:Sociam2015@sotonwo.cloudapp.net:5672")
			.then(
					function(conn) {

						process.once('SIGINT', function() {
							conn.close();
						});
						return conn.createChannel().then(
								function(ch) {
									var ok = ch.assertExchange(queueName,
											'fanout', {
												durable : false
											});

									ok = ok.then(function() {
										return ch.assertQueue('', {
											exclusive : true
										});
									});

									ok = ok.then(function(qok) {
										return ch.bindQueue(qok.queue,
												queueName, '').then(function() {
											return qok.queue;
										});
									});

									ok = ok.then(function(queue) {
										//function(msg,room)
										return ch.consume(queue, function(msg) {
											emitMsg(outName, msg);
										}, {
											noAck : true
										});
									});

									return ok;
								});
					});
};

var connectQueueTwo = function(queueName, outName) {
	return amqp.connect("amqp://wsi-h1.soton.ac.uk").then(function(conn) {

		process.once('SIGINT', function() {
			conn.close();
		});
		return conn.createChannel().then(function(ch) {
			var ok = ch.assertExchange(queueName, 'fanout', {
				durable : false
			});

			ok = ok.then(function() {
				return ch.assertQueue('', {
					exclusive : true
				});
			});

			ok = ok.then(function(qok) {
				return ch.bindQueue(qok.queue, queueName, '').then(function() {
					return qok.queue;
				});
			});

			ok = ok.then(function(queue) {
				//function(msg, room)
				return ch.consume(queue, function(msg) {
					emitMsg(outName, msg);
				}, {
					noAck : true
				});
			});

			return ok;
		});
	});
};

// var connect = connectQueue("wikipedia_hose", "wikipedia_revisions");
// connect = connect.then(function() { return connectQueue("twitter_hose",
// "tweets"); }, showErr);
// connect = connect.then(function() { return connectQueue("trends_hose",
// "trends"); }, showErr);

// for the larger spinn3r connection
var connect = connectQueueTwo("twitter_double", "spinn3r");

// connect = connect.then(function() { return connectQueueTwo("twitter_double",
// "spinn3r"); }, showErr);
// Wiki on the cluster
connect = connect.then(function() {
	return connectQueueTwo("wikipedia_hose", "wikipedia_revisions");
}, showErr);

connect = connect.then(function() {
	return connectQueueTwo("twitter_delete_hose", "twitter_delete");
}, showErr);

connect = connect.then(
		function() {
			return connectQueueTwo("twitter_delete_pulse_hose",
					"twitter_delete_pulse");
		}, showErr);

connect = connect.then(function() {
	return connectQueueTwo("twitter_double", "twitter");
}, showErr);

// connect = connect.then(function() { return connectQueueTwo("news_hose",
// "news"); }, showErr);

connect = connect.then(function() {
	return connectQueueTwo("zooniverse_classifications",
			"zooniverse_classifications");
}, showErr);
connect = connect.then(function() {
	return connectQueueTwo("zooniverse_talk", "zooniverse_talk");
}, showErr);

connect = connect.then(function() {
	return connectQueueTwo("twitter_moocs", "twitter_moocs");
}, showErr);
// connect = connect.then(function() { return connectQueueTwo("twitter_moocs",
// "spinn3r"); }, showErr);
connect = connect.then(function() {
	return connectQueueTwo("twitter_uk_southampton", "spinn3r");
}, showErr);
connect = connect.then(function() {
	return connectQueueTwo("twitter_uk_southampton", "twitter_uk_southampton");
}, showErr);

// Finally, are we ready?
connect = connect.then(function() {
	console.log("Ready at:" + startup_date);
}, showErr);
