function redrawWelcomeChroroplethMap() {

  // get width corresponding to first column in Bootstrap grid
  // this will be used to define the SVG width
  var width = $(".col-xl-8:first-child").width();

  // map has a standard width:height ratio of 960:610
  // SVG height should be calculated based on width and fixed ratio 
  var height = width * 610/960;

  // remove elements within SVG that will hold new choropleth
  d3.selectAll("svg > *").remove();

  // change attributes of SVG element that will hold the choropleth
  var svg = d3.select("#choropleth-welcome")
    .attr("width", width)
    .attr("height", height)

  // create a geo path - https://github.com/mbostock/d3/wiki/Geo-Paths
  var path = d3.geoPath();

  // create a discrete color scale to be used in the choropleth
  var color = d3.scaleQuantize()
    .domain([0, 100])
    .range(d3.schemeReds[9]);

  // create the legend
  var title = "Average number of pills per person per year (2006 through 2012)"
  svg.append("g")
    .attr("transform", `translate(${height}, 20)`)
    .append(() => legend({
      color, 
      title: title,
      tickFormat: ".0f",
    }));

  d3.json("static/data/counties-albers-10m.json").then(function(us) {

    // csv file contains data (average pills per person per year) to be used in the choropleth
    d3.csv("static/data/welcome-choropleth-data.csv").then(function(csvdata) {

      // create an object to get average pills per person by county ID
      // first two digits of county ID is state ID!
      var data = {};
      csvdata.forEach(function(d) {
        data[d.countyfips] = +d.avg_pills_per_person;
      });

      // create an object to get state names by state ID
      var states = {};
      us.objects.states.geometries.forEach(function(d) {
        states[d.id] = d.properties.name;
      });

      // initialize tooltip //
      var tip = d3.tip()
        .attr('class', 'd3-tip')
        .offset([-10, 0])
        .html(function(d) {
          return `<p><strong>${d.properties.name} County, ${states[d.id.slice(0,2)]}</strong>
            <p>${(data[d.id] || 0).toFixed(1)} pills per person per year`;
        })

      // invoke the tip in the context of the visualization //
      svg.call(tip);

      // Draw US counties map using a static TopoJSON file.
      // Since TopoJson file already has cartesian coordinates using the 
      // Albers projection there is no need to use a d3 geo-projection methods.
      // I will use the geoIdentity method to translate the map coordinates 
      // (calculated for a 960 by 610 map) to the container size thanks to the 
      // new fitSize method introduced in d3 version 4.
      var featureCollection = topojson.feature(us, us.objects.counties);
      var projection = d3.geoIdentity()
        .fitSize([width, height], featureCollection);
      var path = d3.geoPath().projection(projection);
      svg.append("g")
          .attr("class", "counties")
          .selectAll("path")
        .data(featureCollection.features)
        .join("path")
          .attr("fill", d => color(data[d.id] || 0))
          .attr("d", path)
          .on('mouseover', tip.show)
          .on('mouseout', tip.hide);
      svg.append("path")
          .attr("class", "county-borders")
          .attr("d", path(topojson.mesh(us, us.objects.counties, function(a, b) { return a !== b; })));

      // changing projection to use feature collection with states 
      // so we can draw the state lines.
      projection = d3.geoIdentity()
        .fitSize([width, height], topojson.feature(us, us.objects.states));
      path = d3.geoPath().projection(projection);
      svg.append("path")
          .attr("class", "state-borders")
          .attr("d", path(topojson.mesh(us, us.objects.states, function(a, b) { return a !== b; })));
    });

  });
}

// draw for the first time to initialize
redrawWelcomeChroroplethMap();

// redraw based on the new size whenever the browser window is resized
window.addEventListener("resize", redrawWelcomeChroroplethMap);
