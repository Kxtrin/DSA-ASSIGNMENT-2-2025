import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerinax/kafka;

configurable string dbHost = "mysql";
configurable int dbPort = 3306;
configurable string dbUser = "root";
configurable string dbPassword = "root123";
configurable string dbName = "dsa_tickets";
configurable string kafkaBootstrap = "kafka:9092";

final mysql:Client db = check new(
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

kafka:ProducerConfiguration producerConfig = {
    clientId: "ticketing-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new(kafkaBootstrap, producerConfig);

type Ticket record {|
    int ticket_id;
    string ticket_number;
    int user_id;
    int route_id;
    int transport_id;
    int schedule_id;
    string travel_date;
    string ticket_type;
    int class_id;
    int number_of_passengers;
    decimal total_price;
    string status;
    string created_at;
|};

type Health record {|
    string status;
    string 'service;
    time:Utc timestamp;
|};

service /ticketing on new http:Listener(8082) {
    
    resource function get health() returns Health {
        return {
            status: "UP",
            'service: "Ticketing Service",
            timestamp: time:utcNow()
        };
    }

    // ==================== CREATE TICKET ====================

    resource function post tickets(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        int userId = check req.user_id;
        int routeId = check req.route_id;
        int transportId = check req.transport_id;
        int scheduleId = check req.schedule_id;
        string travelDate = check req.travel_date;
        string ticketType = check req.ticket_type;
        int classId = check req.class_id;
        int passengers = req.hasKey("number_of_passengers") ? check req.number_of_passengers : 1;
        decimal totalPrice = check req.total_price;

        string ticketNumber = string `TKT-${time:utcNow()[0]}-${uuid:createType1AsString().substring(0, 8)}`;
        
        time:Utc expiryUtc = time:utcAddSeconds(time:utcNow(), 3600);
        string expiryTime = time:utcToString(expiryUtc);

        var result = db->execute(
            `INSERT INTO tickets (ticket_number, user_id, route_id, transport_id, schedule_id,
                                 travel_date, ticket_type, class_id, number_of_passengers, 
                                 total_price, status, expiry_time)
             VALUES (${ticketNumber}, ${userId}, ${routeId}, ${transportId}, ${scheduleId},
                    ${travelDate}, ${ticketType}, ${classId}, ${passengers}, ${totalPrice}, 
                    'created', ${expiryTime})`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        json event = {
            "event_type": "ticket.created",
            "ticket_number": ticketNumber,
            "user_id": userId,
            "route_id": routeId,
            "total_price": totalPrice,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "ticket.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 201;
        resp.setJsonPayload({
            "message": "Ticket created successfully",
            "ticket_number": ticketNumber,
            "status": "created",
            "total_price": totalPrice
        });
        
        log:printInfo("Ticket created", ticket_number = ticketNumber);
        return resp;
    }

    // ==================== GET TICKETS ====================

    resource function get tickets(int? user_id) returns Ticket[]|error {
        stream<Ticket, error?> ticketStream;
        
        if user_id is int {
            ticketStream = db->query(
                `SELECT ticket_id, ticket_number, user_id, route_id, transport_id, schedule_id,
                        DATE_FORMAT(travel_date, '%Y-%m-%d') as travel_date,
                        ticket_type, class_id, number_of_passengers, total_price, status,
                        created_at
                 FROM tickets WHERE user_id = ${user_id}
                 ORDER BY created_at DESC`
            );
        } else {
            ticketStream = db->query(
                `SELECT ticket_id, ticket_number, user_id, route_id, transport_id, schedule_id,
                        DATE_FORMAT(travel_date, '%Y-%m-%d') as travel_date,
                        ticket_type, class_id, number_of_passengers, total_price, status,
                        created_at
                 FROM tickets
                 ORDER BY created_at DESC LIMIT 100`
            );
        }

        return from Ticket t in ticketStream select t;
    }

    resource function get tickets/[string ticketNumber]() returns json|http:NotFound|error {
        stream<Ticket, error?> ticketStream = db->query(
            `SELECT ticket_id, ticket_number, user_id, route_id, transport_id, schedule_id,
                    DATE_FORMAT(travel_date, '%Y-%m-%d') as travel_date,
                    ticket_type, class_id, number_of_passengers, total_price, status,
                    created_at
             FROM tickets WHERE ticket_number = ${ticketNumber}`
        );

        record {|Ticket value;|}|error? result = ticketStream.next();
        check ticketStream.close();

        if result is record {|Ticket value;|} {
            return result.value.toJson();
        }
        return <http:NotFound>{body: "Ticket not found"};
    }

    // ==================== TICKET ACTIONS ====================

    resource function post tickets/[string ticketNumber]/validate() returns http:Response|error {
        http:Response resp = new;

        var result = db->execute(
            `UPDATE tickets 
             SET status = 'validated', validated_at = NOW()
             WHERE ticket_number = ${ticketNumber} AND status = 'paid'`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        json event = {
            "event_type": "ticket.validated",
            "ticket_number": ticketNumber,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "ticket.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 200;
        resp.setJsonPayload({"message": "Ticket validated successfully"});
        log:printInfo("Ticket validated", ticket_number = ticketNumber);
        return resp;
    }

    resource function post tickets/[string ticketNumber]/cancel() returns http:Response|error {
        http:Response resp = new;

        stream<record {string status; int user_id;}, error?> ticketStream = db->query(
            `SELECT status, user_id FROM tickets WHERE ticket_number = ${ticketNumber}`
        );

        record {|record {string status; int user_id;} value;|}|error? ticketResult = ticketStream.next();
        check ticketStream.close();

        if ticketResult is record {|record {string status; int user_id;} value;|} {
            string currentStatus = ticketResult.value.status;
            
            if currentStatus == "validated" {
                resp.statusCode = 400;
                resp.setJsonPayload({"error": "Cannot cancel validated ticket"});
                return resp;
            }

            if currentStatus == "cancelled" {
                resp.statusCode = 400;
                resp.setJsonPayload({"error": "Ticket already cancelled"});
                return resp;
            }

            var result = db->execute(
                `UPDATE tickets 
                 SET status = 'cancelled'
                 WHERE ticket_number = ${ticketNumber}`
            );

            if result is error {
                resp.statusCode = 500;
                resp.setJsonPayload({"error": result.message()});
                return resp;
            }

            json event = {
                "event_type": "ticket.cancelled",
                "ticket_number": ticketNumber,
                "user_id": ticketResult.value.user_id,
                "timestamp": time:utcNow()
            };
            
            check kafkaProducer->send({
                topic: "ticket.events",
                value: event.toJsonString().toBytes()
            });

            resp.statusCode = 200;
            resp.setJsonPayload({"message": "Ticket cancelled successfully"});
            log:printInfo("Ticket cancelled", ticket_number = ticketNumber);
            return resp;
        } else {
            resp.statusCode = 404;
            resp.setJsonPayload({"error": "Ticket not found"});
            return resp;
        }
    }

    // ==================== BOOKINGS (User-friendly view) ====================

    resource function get bookings(int user_id) returns json|error {
        stream<record {}, error?> bookingStream = db->query(
            `SELECT t.ticket_id, t.ticket_number, t.travel_date, t.ticket_type,
                    t.number_of_passengers, t.total_price, t.status, t.created_at
             FROM tickets t
             WHERE t.user_id = ${user_id}
             ORDER BY t.created_at DESC`
        );

        json[] bookings = [];
        check from record {} booking in bookingStream
            do {
                bookings.push(booking.toJson());
            };

        return bookings;
    }
}

// ==================== KAFKA CONSUMER ====================

kafka:ConsumerConfiguration consumerConfig = {
    groupId: "ticketing-service-group",
    topics: ["payment.events"],
    offsetReset: "earliest",
    autoCommit: true
};

service on new kafka:Listener(kafkaBootstrap, consumerConfig) {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            string message = check string:fromBytes(kafkaRecord.value);
            json event = check message.fromJsonString();
            
            map<json> eventMap = check event.ensureType();
            string eventType = check eventMap.event_type;
            
            if eventType == "payment.completed" {
                string ticketNumber = check eventMap.ticket_number;
                
                _ = check db->execute(
                    `UPDATE tickets SET status = 'paid' 
                     WHERE ticket_number = ${ticketNumber}`
                );
                
                log:printInfo("Ticket marked as paid", ticket_number = ticketNumber);
            }
        }
    }
}