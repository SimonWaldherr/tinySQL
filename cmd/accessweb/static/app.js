// AccessWeb – minimal client-side helpers

// Highlight active table link in the sidebar based on the current URL path.
(function () {
  var path = window.location.pathname;
  document.querySelectorAll('.sidebar .table-link').forEach(function (a) {
    if (a.getAttribute('href') === path) {
      a.classList.add('active');
    }
  });
})();
