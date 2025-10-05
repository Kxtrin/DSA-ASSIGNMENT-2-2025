import ballerina/http;
import ballerina/log;
import ballerina/crypto;
import ballerina/time;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string dbHost = "localhost";
configurable int dbPort = 3308;
configurable string dbUser = "root";
configurable string dbPassword = "root123";
configurable string dbName = "dsa_users";

final mysql:Client db = check new(
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

function hashPassword(string password) returns string|error {
    byte[] passwordBytes = password.toBytes();
    byte[] hashBytes = crypto:hashMd5(passwordBytes);
    return hashBytes.toBase16();
}

// User record type
type User record {|
    int user_id;
    string first_name;
    string last_name;
    string email;
|};

type Health record {|
    string status;
    string 'service;
    time:Utc timestamp;
|};

service /passenger on new http:Listener(8080) {
    
    resource function get health() returns Health {
        return {
            status: "UP",
            'service: "Passenger Service",
            timestamp: time:utcNow()
        };
    }

    resource function post register(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        
        map<json> req = check payload.ensureType();
        string email = check req.email;
        string phone = check req.phone;
        string password = check req.password;
        string firstName = req.hasKey("first_name") ? check req.first_name : "Unknown";
        string lastName = req.hasKey("last_name") ? check req.last_name : "User";

        stream<record {|int user_id;|}, error?> existingUser = db->query(
            `SELECT user_id FROM users WHERE email = ${email}`
        );
        
        record {|record {|int user_id;|} value;|}|error? nextRecord = existingUser.next();
        check existingUser.close();
        
        if nextRecord is record {|record {|int user_id;|} value;|} {
            resp.statusCode = 409;
            resp.setJsonPayload({
                "error": "User with this email already exists"
            });
            return resp;
        }

        string hashedPassword = check hashPassword(password);

        _ = check db->execute(
            `INSERT INTO users (user_type, first_name, last_name, email, phone, password_hash)
             VALUES ('client', ${firstName}, ${lastName}, ${email}, ${phone}, ${hashedPassword})`
        );
        
        resp.statusCode = 201;
        resp.setJsonPayload({
            "message": "User registered successfully",
            "email": email
        });
        
        log:printInfo("New user registered", email = email);
        return resp;
    }

    resource function post login(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        
        map<json> req = check payload.ensureType();
        string email = check req.email;
        string password = check req.password;

        string hashedPassword = check hashPassword(password);

        stream<User, error?> userStream = db->query(
            `SELECT user_id, first_name, last_name, email 
             FROM users
             WHERE email = ${email} AND password_hash = ${hashedPassword} AND is_active = TRUE`
        );

        record {|User value;|}|error? nextRecord = userStream.next();
        check userStream.close();

        if nextRecord is record {|User value;|} {
            User userRecord = nextRecord.value;
            log:printInfo("User logged in successfully", email = email);
            
            resp.statusCode = 200;
            resp.setJsonPayload({
                "success": true,
                "user": {
                    "user_id": userRecord.user_id,
                    "first_name": userRecord.first_name,
                    "last_name": userRecord.last_name,
                    "email": userRecord.email
                }
            });
            return resp;
        } else {
            resp.statusCode = 401;
            resp.setJsonPayload({
                "error": "Invalid credentials"
            });
            return resp;
        }
    }
}