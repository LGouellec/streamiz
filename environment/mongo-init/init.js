db = db.getSiblingDB('streamiz');
db.createCollection('adress');
db.adress.insert({ "address": { "city": "Paris", "zip": "123" }, "name": "Mike", "phone": "1234" });
db.adress.insert({ "address": { "city": "Marsel", "zip": "321" }, "name": "Helga", "phone": "4321" });