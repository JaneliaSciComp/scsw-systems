function tableInitialize () {
  $(document).ready(function() {
    $("table").each(function() {
      if ($(this).is('.tablesorter')) {
        $(this).tablesorter();
      }
    });
  });
}

