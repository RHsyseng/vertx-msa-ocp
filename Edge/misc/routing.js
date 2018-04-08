vertx.eventBus().consumer("routing.js", function (message) {
    var jsonObject = map( message.body() );
    message.reply(jsonObject);
});
console.log("Loaded routing.js");

function map(jsonObject) {
    if( jsonObject["host"].startsWith("sales") )
    {
        var ipAddress = jsonObject["forwarded-for"];
        console.log( 'Got IP Address as ' + ipAddress );
        if( ipAddress )
        {
            var lastDigit = ipAddress.substring( ipAddress.length - 1 );
            console.log( 'Got last digit as ' + lastDigit );
            if( lastDigit % 2 != 0 )
            {
                console.log( 'Rerouting to B instance for IP Address ' + ipAddress );
                //Odd IP address, reroute for A/B testing:
                jsonObject["host"] = "sales2:8080";
            }
        }
    }
    return jsonObject;
}

