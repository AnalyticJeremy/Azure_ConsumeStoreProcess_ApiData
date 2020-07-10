var DocumentDBClient = require('documentdb').DocumentClient;
var docdbUtilities = require('./docdbUtilities');

function FederalDocumentUtilities(documentDBClient, databaseId, documentCollectionId, agencyCollectionId) {
    this.client = documentDBClient;
    this.databaseId = databaseId;
    this.documentCollectionId = documentCollectionId;
    this.agencyCollectionId = agencyCollectionId;

    this.database = null;
    this.documentCollection = null;
    this.agencyCollection = null;
}

FederalDocumentUtilities.prototype = {
    init: function (callback) {
        var self = this;

        docdbUtilities.getOrCreateDatabase(self.client, self.databaseId, function (err, db) {
            if (err) {
                callback(err);
            }
            else {
                self.database = db;

                docdbUtilities.getOrCreateCollection(self.client, self.database._self, self.documentCollectionId, function (err, coll) {
                    if (err) {
                        callback(err);
                    }
                    else {
                        self.documentCollection = coll;
                    }
                });

                docdbUtilities.getOrCreateCollection(self.client, self.database._self, self.agencyCollectionId, function (err, coll) {
                    if (err) {
                        callback(err);
                    }
                    else {
                        self.agencyCollection = coll;
                    }
                });
            }
        });
    },

    findDocument: function (querySpec, callback) {
        var self = this;

        if (self.documentCollection != null) {
            self.client.queryDocuments(self.documentCollection._self, querySpec).toArray(function (err, results) {
                if (err) {
                    callback(err);
                }
                else {
                    callback(null, results);
                }
            });
        }
    },

    findAgency: function (querySpec, callback) {
        var self = this;

        if (self.agencyCollection != null) {
            self.client.queryDocuments(self.agencyCollection._self, querySpec).toArray(function (err, results) {
                if (err) {
                    callback(err);
                }
                else {
                    callback(null, results);
                }
            });
        }
    },

    addItem: function (item, callback) {
        var self = this;

        item.date = Date.now();
        item.completed = false;

        self.client.createDocument(self.documentCollection._self, item, function (err, doc) {
            if (err) {
                callback(err);
            }
            else {
                callback(null, doc);
            }
        });
    },

    updateFederalDocumentRating: function (data, callback) {
        var self = this;
        self.updateFederalDocument(data, callback, function (data, federalDocument) {
            var timestamp = new Date().toISOString();
            federalDocument.actual_rating = data.rating;
            federalDocument.date_rated = timestamp;
            federalDocument.date_reviewed = timestamp;
            federalDocument.date_update = timestamp;
        });
    },

    updateFederalDocumentCategory: function (data, callback) {
        var self = this;
        self.updateFederalDocument(data, callback, function (data, federalDocument) {
            var timestamp = new Date().toISOString();
            federalDocument.actual_category = data.category;
            federalDocument.date_categorized = timestamp;
            federalDocument.date_reviewed = timestamp;
            federalDocument.date_update = timestamp;
        });
    },

    updateFederalDocumentAsExpanded: function (data, callback) {
        var self = this;
        self.updateFederalDocument(data, callback, function (data, federalDocument) {
            var timestamp = new Date().toISOString();
            federalDocument.date_expanded = timestamp;
            federalDocument.date_reviewed = timestamp;
            federalDocument.date_update = timestamp;
        });
    },

    updateFederalDocumentAsPdfRead: function (data, callback) {
        var self = this;
        self.updateFederalDocument(data, callback, function (data, federalDocument) {
            var timestamp = new Date().toISOString();
            federalDocument.date_pdf_read = timestamp;
            federalDocument.date_reviewed = timestamp;
            federalDocument.date_update = timestamp;
        });
    },

    updateFederalDocument: function (data, callback, updateFunction) {
        var self = this;

        self.getFederalDocument(data.id, function (err, federalDocument) {
            if (err) {
                callback(err);
            }
            else {
                updateFunction(data, federalDocument);

                self.client.replaceDocument(federalDocument._self, federalDocument, function (err, replaced) {
                    if (err) {
                        callback(err);
                    }
                    else {
                        callback(null, replaced);
                    }
                });
            }
        });
    },

    getFederalDocument: function (documentId, callback) {
        var self = this;

        var querySpec = {
            query: 'SELECT * FROM root r WHERE r.id = @id',
            parameters: [{
                name: '@id',
                value: documentId
            }]
        };

        self.client.queryDocuments(self.documentCollection._self, querySpec).toArray(function (err, results) {
            if (err) {
                callback(err);
            }
            else {
                callback(null, results[0]);
            }
        });
    }
};

module.exports = FederalDocumentUtilities;
