$(function () {
    $('.rateit')
        .rateit('max', 3)
        .rateit('step', 1)
        .rateit('resetable', false)
        .bind('rated', rateit_rated);

    $('select.category-list').bind('change', category_changed);
    $('button.doc-expand-btn').bind('click', document_expanded)
    $('a.pdf-link').bind('click', link_clicked)
});

function rateit_rated() {
    var item = $(this);
    var rating = item.rateit('value');
    var documentId = item.data('documentid');
    var data = { id: documentId, rating: rating };

    $.post("/rate", data)
        .done(function (data) {
            console.log("Data Saved: " + data);
        }).fail(function (jqXhr, status, error) {
            alert("An error occurred while trying to save your document rating to the database.\n\n\nDetails: " + JSON.stringify(jqXhr));
        });
}

function category_changed(e) {
    var item = $(this);
    var category = this.value
    var documentId = item.data('documentid');
    var data = { id: documentId, category: category };

    $.post("/categorize", data)
        .done(function (data) {
            console.log("Data Saved: " + data);
        }).fail(function (jqXhr, status, error) {
            alert("An error occurred while trying to save your document category to the database.\n\n\nDetails: " + JSON.stringify(jqXhr));
        });
}

function document_expanded(e) {
    var item = $(this);
    var documentId = item.data('documentid');
    var data = { id: documentId };

    $.post("/markexpanded", data)
        .done(function (data) {
            console.log("Data Saved: " + data);
        }).fail(function (jqXhr, status, error) {
            alert("An error occurred while trying to mark your document as expanded.\n\n\nDetails: " + JSON.stringify(jqXhr));
        });
}

function link_clicked(e) {
    var item = $(this);
    var documentId = item.data('documentid');
    var data = { id: documentId };

    $.post("/markpdfread", data)
        .done(function (data) {
            console.log("Data Saved: " + data);
        }).fail(function (jqXhr, status, error) {
            alert("An error occurred while trying to mark your document as PDF read.\n\n\nDetails: " + JSON.stringify(jqXhr));
        });
}
