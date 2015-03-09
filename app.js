var amqp = require('amqplib');
var app = require('http').createServer(handler);
var io = require('socket.io')(app);
var fs = require('fs');
var _ = require('underscore');
var wpimg = require('wikipedia-image');
//var config = require('./config');

app.listen(9001);

var active_users = 0;
var temp_user_cnt = 0;

var keyword_filter = "";
var language_filter = "";

var processed_msg_cnt = 0;


function showErr (e) {
    console.error(e, e.stack);
}

function handler (req, res) {
    res.writeHead(200);
    res.end("");
}

var filter = {
    "filter": false,
}; // global filter state, and default



//SOCKET DETAILS
//Current socketnames in use:
    // user_heartbeat
    // active_user
    // filter
    // filter_keyword
    // set_filter_keyword
    // filter_lang
    // set_filter_lang
    // processed_messages;

//Hoses (Mappings from RabbitMQ)
    //trends
    //news
    //twitter
    //spinn3r
    //wikipedia_revisions
    //wikipedia_images
    //twitter_delete
    //twitter_delete_pulse


io.on('connection', function (socket) {
    socket.emit("filter", filter); // emit the current state to this client

    // receive a filter update, combine it and send to ALL clients
    socket.on('filter', function (newFilter) {
        //console.log("filter updated:", newFilter);
        _.extend(filter, newFilter);
        //console.log("emitting filter:", filter); 
        io.emit("filter", filter);
        io.emit("set_filter_keyword", keyword_filter);
        io.emit("set_filter_lang", language_filter);

    });

    // receive a filter update, combine it and send to ALL clients
    socket.on('filter_keyword', function (newFilter) {
        //console.log("filter updated:", newFilter);
        filter = true;
        keyword_filter = newFilter;
        //console.log("emitting filter:", filter); 
        io.emit("filter", filter);
        io.emit("set_filter_keyword", keyword_filter);

    });


  // receive a filter update, combine it and send to ALL clients
    socket.on('filter_lang', function (newFilter) {
        //console.log("filter updated:", newFilter);
        filter = true;
        language_filter = newFilter;
        //console.log("emitting filter:", filter); 
        io.emit("filter", filter);
        io.emit("set_filter_lang", language_filter);
    });

    //new ms user...
    // receive a filter update, combine it and send to ALL clients
    socket.on('active_user', function (newFilter) {
        //console.log("filter updated:", newFilter);
        //console.log("emitting filter:", filter); 
        ++temp_user_cnt;
    });

});

function emit_processed_message_count(){
    io.emit("processed_msg_cnt", processed_msg_cnt);
}
//reset filters every 60 seconds - just for sanity...
var processed_msgcnt_interval = setInterval(function(){emit_processed_message_count()}, 2000);



//reset all filters
function resetKeywords(){

	keyword_filter = "";
	language_filter = "";

	//let the clients know
	io.emit("set_filter_keyword", keyword_filter);
	io.emit("set_filter_lang", language_filter);
}

//reset filters every 60 seconds - just for sanity...
var resetKeywords_interval = setInterval(function(){resetKeywords()}, 120000);



//Update the master count with the temp count
function updateUserCount(){
    active_users = temp_user_cnt;
}

//want to send a heartbeat to all users to see how many are still connected!
function checkForUsers() {
    //send a heartbeat
    temp_user_cnt = 0;
     io.emit("user_heartbeat", filter);
}

//want to send a heartbeat to all users to see how many are still connected!
function emitUserCount() {
    //send a heartbeat
     io.emit("active_user_count", active_users);
}


//These Control the User Count Details
var checkForUsers_interval = setInterval(function(){checkForUsers()}, 1000);
var updateUserCount_interval = setInterval(function(){updateUserCount()}, 2000);
var emit_userCount = setInterval(function(){emitUserCount()}, 1000);



function checkFilters(msg){

    var match_keyword = true;
    var match_lang = true;

    //is the filter enabled?
    if(filter){


        //check for keyword filter
        if(msg.content.toString().indexOf(keyword_filter) > -1){
            match_keyword = true;
        }else{
            match_keyword = false;
        }

        if(language_filter.length>0){

           // var data = JSON.parse(msg.content.toString());
            try{
                    if(msg.content.toString().indexOf('"'+language_filter+'"')>-1){
                        match_lang = true;
                    }else{
                        match_lang = false;
                    }
                }catch(e){
                        console.log(e)
                        match_lang = false;    
                }
        }

        if(match_lang && match_keyword){
            return true;
        }else{
            return false;
        }


    }else{

        return true;
    }


}

//here we worry about the message sending
//We perform raw filtering here!
var emitMsg = function (outName, msg) {
    try {
        ++processed_msg_cnt;
        //do a raw match on the message
        
        if(checkFilters(msg)){

            var data = JSON.parse(msg.content.toString());
            io.emit(outName, data);

        
            //make the revisions images feed
            if (outName == "wikipedia_revisions") {
                var page_url = data.wikipedia_page_url;
                if (page_url) {
    		      	wpimg(page_url).then(function (image) {
                        		if (image && image != "") {
                            	io.emit('wikipedia_images', {"image_url": image, "data": data});
                        	}
    		
                    	}, function (e) {
                        	// error querying etc
                    	});
    		}
            }

        //end of filter
        }

    } catch (e) {
        //
    }
}

var connectQueue = function (queueName, outName) {
    return amqp.connect("amqp://localhost").then(function(conn) {

        process.once('SIGINT', function() { conn.close(); });
        return conn.createChannel().then(function(ch) {
            var ok = ch.assertExchange(queueName, 'fanout', {durable: false});

            ok = ok.then(function() { return ch.assertQueue('', {exclusive: true}); });

            ok = ok.then(function(qok) {
                return ch.bindQueue(qok.queue, queueName, '').then(function() {
                    return qok.queue;
                });
            });

            ok = ok.then(function(queue) {
                return ch.consume(queue, function (msg) { emitMsg(outName, msg); }, {noAck: true});
            });

            return ok;
        });
    });
};


var connectQueueTwo = function (queueName, outName) {
    return amqp.connect("amqp://wsi-h1.soton.ac.uk").then(function(conn) {

        process.once('SIGINT', function() { conn.close(); });
        return conn.createChannel().then(function(ch) {
            var ok = ch.assertExchange(queueName, 'fanout', {durable: false});

            ok = ok.then(function() { return ch.assertQueue('', {exclusive: true}); });

            ok = ok.then(function(qok) {
                return ch.bindQueue(qok.queue, queueName, '').then(function() {
                    return qok.queue;
                });
            });

            ok = ok.then(function(queue) {
                return ch.consume(queue, function (msg) { emitMsg(outName, msg); }, {noAck: true});
            });

            return ok;
        });
    });
};

var connect = connectQueue("logs", "tweets");
//connect = connect.then(function() { return connectQueue("twitter_hose", "tweets"); }, showErr);
connect = connect.then(function() { return connectQueue("trends_hose", "trends"); }, showErr);

//for the larger spinn3r connection
connect = connect.then(function() { return connectQueueTwo("spinn3r_hose", "spinn3r"); }, showErr);
//Wiki on the cluster
connect = connect.then(function() { return connectQueueTwo("wikipedia_hose", "wikipedia_revisions"); }, showErr);

connect = connect.then(function() { return connectQueueTwo("twitter_delete_hose", "twitter_delete"); }, showErr);

connect = connect.then(function() { return connectQueueTwo("twitter_delete_pulse_hose", "twitter_delete_pulse"); }, showErr);


connect = connect.then(function() { return connectQueueTwo("logs", "twitter"); }, showErr);

connect = connect.then(function() { return connectQueueTwo("news_hose", "news"); }, showErr);


//Finally, are we ready?
connect = connect.then(function() { console.log("Ready."); }, showErr);
