var DocumentDBClient = require('documentdb').DocumentClient;
var async = require('async');

function FederalDocumentList(federalDocumentUtilities) {
    this.federalDocumentUtilities = federalDocumentUtilities;
}

FederalDocumentList.prototype = {
    showFederalDocuments: function (req, res) {
        var self = this;

        var filterDate = req.query["d"];
        var dayFactor = 0;
        filterDate = new Date(filterDate + " 00:00:00");
        if (isNaN(filterDate) == true || filterDate > new Date()) {
            filterDate = new Date();
            dayFactor = -1;
        }
        if (filterDate.getDay() == 0) {
            dayFactor = -2;
        }
        else if (filterDate.getDay() == 1) {
            dayFactor = -3;
        }
        filterDate.setDate(filterDate.getDate() + dayFactor);
        filterDate = filterDate.toISOString().substring(0, 10);

        async.parallel(
            {
                "agencies": function (callback) {
                    var agencyQuerySpec = {
                        query: 'SELECT * FROM root r'
                    };

                    self.federalDocumentUtilities.findAgency(agencyQuerySpec, callback);
                },
                "documents": function (callback) {
                    var documentQuerySpec = {
                        query: 'SELECT r.id, r.title, r.type, r.abstract_text, r.pdf_url, r.predicted_category["Scored Labels"] AS predicted_category, r.publication_date, r.agency_ids, r.key_phrases, r.predicted_interest_score, r.actual_category, r.actual_rating, r.date_categorized FROM root r WHERE r.publication_date = @pub_date',
                        parameters: [{
                            name: '@pub_date',
                            value: filterDate
                        }]
                    };

                    self.federalDocumentUtilities.findDocument(documentQuerySpec, callback);
                }
            },
            function (err, results) {
                if (err) {
                    throw (err);
                }

                // use the agency data to build a hash that lets us look up an agency's name based on its ID
                var agencies = results["agencies"];
                var agencyHash = [];
                for (var i = 0; i < agencies.length; i++) {
                    var agency = agencies[i];
                    agencyHash[agency.id] = agency.name;
                }

                // for all of the documents, add a new array that contains the agency names
                var documents = results["documents"];
                for (var i = 0; i < documents.length; i++) {
                    var document = documents[i];
                    document.agencies = [];
                    for (var j = 0; j < document.agency_ids.length; j++) {
                        var agencyId = document.agency_ids[j];
                        document.agencies.push(agencyHash[agencyId]);
                    }
                }

                // group the documents
                var groupedDocuments = {};
                groupedDocuments.categoryNames = [];
                groupedDocuments.categories = [];
                groupedDocuments.totalDocumentCount = documents.length;

                for (var i = 0; i < documents.length; i++) {
                    var document = documents[i];
                    var categoryName = document.actual_category;
                    var category = null;

                    if (document.date_categorized == null) {
                        categoryName = document.predicted_category;
                    }

                    var categoryIndex = groupedDocuments.categoryNames.indexOf(categoryName);
                    if (categoryIndex >= 0) {
                        category = groupedDocuments.categories[categoryIndex];
                    }
                    else {
                        category = {};
                        category.name = categoryName;
                        category.idName = categoryName.replace(/[^A-z0-9]/g, '-');
                        category.agencyNames = [];
                        category.agencies = [];
                        groupedDocuments.categoryNames.push(categoryName);
                        groupedDocuments.categories.push(category);
                    }

                    for (var j = 0; j < document.agencies.length; j++) {
                        var agencyName = document.agencies[j];
                        var agency = null;

                        var agencyIndex = category.agencyNames.indexOf(agencyName);
                        if (agencyIndex >= 0) {
                            agency = category.agencies[agencyIndex];
                        }
                        else {
                            agency = {};
                            agency.name = agencyName;
                            agency.idName = agencyName.replace(/ /g, '-').replace(/&/g, 'and');
                            agency.documents = [];
                            category.agencyNames.push(agencyName);
                            category.agencies.push(agency);
                        }

                        agency.documents.push(document);
                    }
                }

                // sort the groupings and documents within groupings
                groupedDocuments.categories.sort(function (l, r) {
                    return l.name.localeCompare(r.name);
                });

                for (var i = 0; i < groupedDocuments.categories.length; i++) {
                    var category = groupedDocuments.categories[i];

                    category.agencies.sort(function (l, r) {
                        return l.name.localeCompare(r.name);
                    });

                    for (var j = 0; j < category.agencies.length; j++) {
                        category.agencies[j].documents.sort(function (l, r) {
                            if (l.type != r.type) {
                                return l.type.localeCompare(r.type);
                            }
                            else {
                                return l.id.localeCompare(r.id);
                            }
                        });
                    }
                }

                res.render('index', {
                    title: 'Federal Documents for Review',
                    federalDocuments: documents,
                    groupedDocuments: groupedDocuments,
                    filterDate: filterDate
                });
            }
        );
    },

    addFederalDocument: function (req, res) {
        var self = this;
        var item = req.body;

        self.federalDocumentUtilities.addItem(item, function (err) {
            if (err) {
                throw (err);
            }

            res.redirect('/');
        });
    },

    rateFederalDocument: function (req, res) {
        var self = this;
        var data = req.body;
        
        self.federalDocumentUtilities.updateFederalDocumentRating(data, function (err) {
            if (err) {
                callback(err);
            }
            else {
                res.send('{ message: "Document Rating Saved!",  id: "' + data.id + '", rating: ' + data.rating + ' }');
            }
        });
    },

    categorizeFederalDocument: function (req, res) {
        var self = this;
        var data = req.body;

        self.federalDocumentUtilities.updateFederalDocumentCategory(data, function (err) {
            if (err) {
                callback(err);
            }
            else {
                res.send('{ message: "Document Category Saved!",  id: "' + data.id + '", category: "' + data.category + '" }');
            }
        });
    },

    markFederalDocumentExpanded: function (req, res) {
        var self = this;
        var data = req.body;

        self.federalDocumentUtilities.updateFederalDocumentAsExpanded(data, function (err) {
            if (err) {
                callback(err);
            }
            else {
                res.send('{ message: "Document Marked As Expanded!",  id: "' + data.id + '" }');
            }
        });
    },

    markFederalDocumentPdfRead: function (req, res) {
        var self = this;
        var data = req.body;

        self.federalDocumentUtilities.updateFederalDocumentAsPdfRead(data, function (err) {
            if (err) {
                callback(err);
            }
            else {
                res.send('{ message: "Document Marked As PDF Read!",  id: "' + data.id + '" }');
            }
        });
    }
};

module.exports = FederalDocumentList;