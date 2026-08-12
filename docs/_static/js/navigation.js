$(function () {
  const menu = $(".wy-menu-vertical");
  const collapseAll = $(
    '<button type="button" class="sidebar-collapse-all" ' +
      'title="Collapse all sections" aria-label="Collapse all sections">' +
      '<span aria-hidden="true">&minus;</span></button>'
  );

  menu.prepend(collapseAll);
  collapseAll.on("click", function () {
    menu
      .find("li.current, ul.current")
      .removeClass("current")
      .attr("aria-expanded", "false");
  });

  SphinxRtdTheme.Navigation.toggleCurrent = function (link) {
    const item = link.closest("li");
    const descendants = item.find("> ul li");

    if (!descendants.length) {
      return;
    }

    const expanded = item.attr("aria-expanded") !== "true";
    item.toggleClass("current", expanded).attr("aria-expanded", expanded);

    if (!expanded) {
      descendants.removeClass("current").attr("aria-expanded", "false");
    }
  };
});
