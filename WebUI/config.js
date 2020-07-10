var config = {}

config.host = process.env.HOST || "https://<YOUR COSMOS DB INSTANCE NAME>.documents.azure.com:443/";
config.authKey = process.env.AUTH_KEY || "YOUR COSMOS DB KEY";
config.databaseId = "YOUR COSMOS DB DATABASE NAME";
config.documentCollectionId = "documents";
config.agencyCollectionId = "agencies";

module.exports = config;
