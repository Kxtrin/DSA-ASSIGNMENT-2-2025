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
configurable string dbName = "dsa_payments";
configurable string kafkaBootstrap = "kafka:9092";

final mysql:Client db = check new(
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

kafka:ProducerConfiguration producerConfig = {
    clientId: "payment-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer kafkaProducer = check new(kafkaBootstrap, producerConfig);

type Payment record {|
    int payment_id;
    int ticket_id;
    int user_id;
    decimal amount;
    string payment_type;
    string payment_status;
    string transaction_reference;
    string created_at;
|};

type Health record {|
    string status;
    string 'service;
    time:Utc timestamp;
|};

service /payment on new http:Listener(8083) {
    
    resource function get health() returns Health {
        return {
            status: "UP",
            'service: "Payment Service",
            timestamp: time:utcNow()
        };
    }

    resource function post payments(@http:Payload json payload) returns http:Response|error {
        http:Response resp = new;
        map<json> req = check payload.ensureType();
        
        int ticketId = check req.ticket_id;
        int userId = check req.user_id;
        decimal amount = check req.amount;
        string paymentType = check req.payment_type;
        string ticketNumber = check req.ticket_number;

        string transactionRef = string `TXN-${time:utcNow()[0]}-${uuid:createType1AsString().substring(0, 8)}`;

        var result = db->execute(
            `INSERT INTO payments (ticket_id, user_id, amount, payment_type, 
                                  payment_status, transaction_reference)
             VALUES (${ticketId}, ${userId}, ${amount}, ${paymentType}, 
                    'pending', ${transactionRef})`
        );

        if result is error {
            resp.statusCode = 500;
            resp.setJsonPayload({"error": result.message()});
            return resp;
        }

        boolean paymentSuccess = simulatePaymentProcessing(paymentType);

        if paymentSuccess {
            _ = check db->execute(
                `UPDATE payments 
                 SET payment_status = 'paid', payment_date = NOW()
                 WHERE transaction_reference = ${transactionRef}`
            );

            json event = {
                "event_type": "payment.completed",
                "ticket_number": ticketNumber,
                "transaction_reference": transactionRef,
                "amount": amount,
                "user_id": userId,
                "timestamp": time:utcNow()
            };
            
            check kafkaProducer->send({
                topic: "payment.events",
                value: event.toJsonString().toBytes()
            });

            resp.statusCode = 200;
            resp.setJsonPayload({
                "message": "Payment processed successfully",
                "transaction_reference": transactionRef,
                "status": "paid"
            });
            
            log:printInfo("Payment completed", transaction_ref = transactionRef);
        } else {
            _ = check db->execute(
                `UPDATE payments 
                 SET payment_status = 'failed'
                 WHERE transaction_reference = ${transactionRef}`
            );

            resp.statusCode = 400;
            resp.setJsonPayload({
                "error": "Payment processing failed",
                "transaction_reference": transactionRef,
                "status": "failed"
            });
            
            log:printError("Payment failed", transaction_ref = transactionRef);
        }

        return resp;
    }

    resource function get payments(int? user_id) returns Payment[]|error {
        stream<Payment, error?> paymentStream;
        
        if user_id is int {
            paymentStream = db->query(
                `SELECT payment_id, ticket_id, user_id, amount, payment_type,
                        payment_status, transaction_reference, created_at
                 FROM payments WHERE user_id = ${user_id}
                 ORDER BY created_at DESC`
            );
        } else {
            paymentStream = db->query(
                `SELECT payment_id, ticket_id, user_id, amount, payment_type,
                        payment_status, transaction_reference, created_at
                 FROM payments
                 ORDER BY created_at DESC LIMIT 100`
            );
        }

        return from Payment p in paymentStream select p;
    }

    resource function get payments/[string transactionRef]() returns json|http:NotFound|error {
        stream<Payment, error?> paymentStream = db->query(
            `SELECT payment_id, ticket_id, user_id, amount, payment_type,
                    payment_status, transaction_reference, created_at
             FROM payments WHERE transaction_reference = ${transactionRef}`
        );

        record {|Payment value;|}|error? result = paymentStream.next();
        check paymentStream.close();

        if result is record {|Payment value;|} {
            return result.value.toJson();
        }
        return <http:NotFound>{body: "Payment not found"};
    }
}

function simulatePaymentProcessing(string paymentType) returns boolean {
    if paymentType == "card" || paymentType == "mobile_money" {
        return true;
    }
    return true;
}
