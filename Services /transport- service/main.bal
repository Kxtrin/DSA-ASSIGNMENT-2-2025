import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerinax/kafka;

configurable string dbHost = "mysql";
configurable int dbPort = 3306;
configurable string dbUser = "root";
configurable string dbPassword = "root123";
configurable string dbName = "dsa_routes";
configurable string kafkaBootstrap = "kafka:9092";

final mysql:Client db = check new(
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

kafka:ProducerConfiguration producerConfig = {
    clientId: "transport-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new(kafkaBootstrap, producerConfig);

type Transport record {|
    int transport_id;
    string transport_type;
    string brand;
    string registration_number;
    int total_capacity;
    boolean is_active;
|};

type Route record {|
    int route_id;
    string route_name;
    string start_point;
    string end_point;
    decimal distance_km;
    decimal base_price;
    boolean is_active;
|};

type Schedule record {|
    int schedule_id;
    int route_id;
    int transport_id;
    string departure_time;
    string arrival_time;
    string operating_days;
    string effective_from;
    string? effective_to;
    boolean is_active;
|};

type TransportClass record {|
    int class_id;
    int transport_id;
    string class_name;
    int seat_count;
    decimal price_multiplier;
|};

type Health record {|
    string status;
    string 'service;
    time:Utc timestamp;
|};

service /transport on new http:Listener(8081) {
    
    resource function get health() returns Health {
        return {
            status: "UP",
            'service: "Transport Service",
            timestamp: time:utcNow()
        };
    }

    // ==================== TRANSPORTS ====================

    resource function post transports(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        string transportType = check req.transport_type;
        string brand = check req.brand;
        string regNumber = check req.registration_number;
        int capacity = check req.total_capacity;

        _ = check db->execute(
            `INSERT INTO transports (transport_type, brand, registration_number, total_capacity)
             VALUES (${transportType}, ${brand}, ${regNumber}, ${capacity})`
        );

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Transport created successfully"});
        log:printInfo("Transport created", registration = regNumber);
        return resp;
    }

    resource function get transports() returns Transport[]|error {
        stream<Transport, error?> transportStream = db->query(
            `SELECT transport_id, transport_type, brand, registration_number, 
                    total_capacity, is_active 
             FROM transports WHERE is_active = TRUE`
        );

        return from Transport t in transportStream select t;
    }

    resource function get transports/[int id]() returns json|http:NotFound|error {
        stream<Transport, error?> transportStream = db->query(
            `SELECT transport_id, transport_type, brand, registration_number, 
                    total_capacity, is_active 
             FROM transports WHERE transport_id = ${id}`
        );

        record {|Transport value;|}|error? result = transportStream.next();
        check transportStream.close();

        if result is record {|Transport value;|} {
            return result.value.toJson();
        }
        return <http:NotFound>{body: "Transport not found"};
    }

    // ==================== ROUTES ====================

    resource function post routes(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        string routeName = check req.route_name;
        string startPoint = check req.start_point;
        string endPoint = check req.end_point;
        decimal distanceKm = check req.distance_km;
        decimal basePrice = check req.base_price;

        _ = check db->execute(
            `INSERT INTO routes (route_name, start_point, end_point, distance_km, base_price)
             VALUES (${routeName}, ${startPoint}, ${endPoint}, ${distanceKm}, ${basePrice})`
        );

        json event = {
            "event_type": "route.created",
            "route_name": routeName,
            "start_point": startPoint,
            "end_point": endPoint,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "route.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Route created successfully"});
        log:printInfo("Route created", route = routeName);
        return resp;
    }

    resource function get routes() returns Route[]|error {
        stream<Route, error?> routeStream = db->query(
            `SELECT route_id, route_name, start_point, end_point, 
                    distance_km, base_price, is_active 
             FROM routes WHERE is_active = TRUE`
        );

        return from Route r in routeStream select r;
    }

    resource function get routes/[int id]() returns json|http:NotFound|error {
        stream<Route, error?> routeStream = db->query(
            `SELECT route_id, route_name, start_point, end_point, 
                    distance_km, base_price, is_active 
             FROM routes WHERE route_id = ${id}`
        );

        record {|Route value;|}|error? result = routeStream.next();
        check routeStream.close();

        if result is record {|Route value;|} {
            return result.value.toJson();
        }
        return <http:NotFound>{body: "Route not found"};
    }

    // ==================== SCHEDULES ====================

    resource function post schedules(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        int routeId = check req.route_id;
        int transportId = check req.transport_id;
        string departureTime = check req.departure_time;
        string arrivalTime = check req.arrival_time;
        string operatingDays = check req.operating_days;
        string effectiveFrom = check req.effective_from;
        string? effectiveTo = req.hasKey("effective_to") ? check req.effective_to : ();

        if effectiveTo is string {
            _ = check db->execute(
                `INSERT INTO schedules (route_id, transport_id, departure_time, arrival_time, 
                                       operating_days, effective_from, effective_to)
                 VALUES (${routeId}, ${transportId}, ${departureTime}, ${arrivalTime}, 
                        ${operatingDays}, ${effectiveFrom}, ${effectiveTo})`
            );
        } else {
            _ = check db->execute(
                `INSERT INTO schedules (route_id, transport_id, departure_time, arrival_time, 
                                       operating_days, effective_from)
                 VALUES (${routeId}, ${transportId}, ${departureTime}, ${arrivalTime}, 
                        ${operatingDays}, ${effectiveFrom})`
            );
        }

        json event = {
            "event_type": "schedule.created",
            "route_id": routeId,
            "transport_id": transportId,
            "timestamp": time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "schedule.events",
            value: event.toJsonString().toBytes()
        });

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Schedule created successfully"});
        log:printInfo("Schedule created", route_id = routeId, transport_id = transportId);
        return resp;
    }

    resource function get schedules(int? route_id, string? travel_date) returns json|error {
        stream<record {}, error?> scheduleStream;
        
        if route_id is int && travel_date is string {
            scheduleStream = db->query(
                `SELECT s.schedule_id, s.route_id, s.transport_id, s.departure_time, 
                        s.arrival_time, s.operating_days, s.effective_from, s.effective_to,
                        r.route_name, r.start_point, r.end_point, r.base_price,
                        t.transport_type, t.brand, t.total_capacity
                 FROM schedules s
                 JOIN routes r ON s.route_id = r.route_id
                 JOIN transports t ON s.transport_id = t.transport_id
                 WHERE s.route_id = ${route_id} 
                   AND s.is_active = TRUE
                   AND r.is_active = TRUE
                   AND t.is_active = TRUE
                   AND s.effective_from <= ${travel_date}
                   AND (s.effective_to IS NULL OR s.effective_to >= ${travel_date})`
            );
        } else if route_id is int {
            scheduleStream = db->query(
                `SELECT s.schedule_id, s.route_id, s.transport_id, s.departure_time, 
                        s.arrival_time, s.operating_days, s.effective_from, s.effective_to,
                        r.route_name, r.start_point, r.end_point, r.base_price,
                        t.transport_type, t.brand, t.total_capacity
                 FROM schedules s
                 JOIN routes r ON s.route_id = r.route_id
                 JOIN transports t ON s.transport_id = t.transport_id
                 WHERE s.route_id = ${route_id} 
                   AND s.is_active = TRUE
                   AND r.is_active = TRUE
                   AND t.is_active = TRUE`
            );
        } else {
            scheduleStream = db->query(
                `SELECT s.schedule_id, s.route_id, s.transport_id, s.departure_time, 
                        s.arrival_time, s.operating_days, s.effective_from, s.effective_to,
                        r.route_name, r.start_point, r.end_point, r.base_price,
                        t.transport_type, t.brand, t.total_capacity
                 FROM schedules s
                 JOIN routes r ON s.route_id = r.route_id
                 JOIN transports t ON s.transport_id = t.transport_id
                 WHERE s.is_active = TRUE
                   AND r.is_active = TRUE
                   AND t.is_active = TRUE
                 ORDER BY s.route_id, s.departure_time
                 LIMIT 100`
            );
        }

        json[] schedules = [];
        check from record {} schedule in scheduleStream
            do {
                schedules.push(schedule.toJson());
            };

        return schedules;
    }

    resource function get schedules/[int id]() returns json|http:NotFound|error {
        stream<record {}, error?> scheduleStream = db->query(
            `SELECT s.schedule_id, s.route_id, s.transport_id, s.departure_time, 
                    s.arrival_time, s.operating_days, s.effective_from, s.effective_to,
                    r.route_name, r.start_point, r.end_point, r.base_price,
                    t.transport_type, t.brand, t.total_capacity
             FROM schedules s
             JOIN routes r ON s.route_id = r.route_id
             JOIN transports t ON s.transport_id = t.transport_id
             WHERE s.schedule_id = ${id}`
        );

        record {|record {} value;|}|error? result = scheduleStream.next();
        check scheduleStream.close();

        if result is record {|record {} value;|} {
            return result.value.toJson();
        }
        return <http:NotFound>{body: "Schedule not found"};
    }

    // ==================== TRANSPORT CLASSES ====================

    resource function post classes(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        int transportId = check req.transport_id;
        string className = check req.class_name;
        int seatCount = check req.seat_count;
        decimal priceMultiplier = req.hasKey("price_multiplier") ? 
            check req.price_multiplier : 1.00;

        _ = check db->execute(
            `INSERT INTO transport_classes (transport_id, class_name, seat_count, price_multiplier)
             VALUES (${transportId}, ${className}, ${seatCount}, ${priceMultiplier})`
        );

        resp.statusCode = 201;
        resp.setJsonPayload({"message": "Transport class created successfully"});
        log:printInfo("Transport class created", transport_id = transportId, 'class = className);
        return resp;
    }

    resource function get classes(int? transport_id) returns TransportClass[]|error {
        stream<TransportClass, error?> classStream;
        
        if transport_id is int {
            classStream = db->query(
                `SELECT class_id, transport_id, class_name, seat_count, price_multiplier
                 FROM transport_classes WHERE transport_id = ${transport_id}`
            );
        } else {
            classStream = db->query(
                `SELECT class_id, transport_id, class_name, seat_count, price_multiplier
                 FROM transport_classes`
            );
        }

        return from TransportClass c in classStream select c;
    }
}