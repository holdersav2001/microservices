db = db.getSiblingDB('realtime');

db.trade_events.createIndex({ "eventName": 1, "eventStatus": 1 });
db.trade_events.createIndex({ "businessDate": 1 });
db.trade_events.createIndex({ "eventName": 1, "eventStatus": 1, "businessDate": 1 });
db.trade_events.createIndex({ "type": 1 });
db.trade_events.createIndex({ "timestamp": 1 });
db.trade_events.createIndex({ "tradeId": 1 });

print("MongoDB indexes have been created for the realtime database and trade_events collection.");