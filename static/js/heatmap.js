var myMap = L.map("map", {
  center: [41.4925, -99.9018],
  zoom: 4
});

L.tileLayer("https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}", {
  attribution: "Map data &copy; <a href='https://www.openstreetmap.org/'>OpenStreetMap</a> contributors, <a href='https://creativecommons.org/licenses/by-sa/2.0/'>CC-BY-SA</a>, Imagery © <a href='https://www.mapbox.com/'>Mapbox</a>",
  maxZoom: 18,
  id: "mapbox.streets",
  accessToken: MAPBOX_API_KEY
}).addTo(myMap);

// Use the list of sample names to populate the select options
// d3.json("/heat").then(function(err, data) {
d3.json('/heatlist', function(response) {  

  var heatArray = [];

  for (var i = 0; i < response.len; i++) {
      heatArray.push([response['lat'][i], response['lon'][i], response['addiction_index'][i]*4]);
  }

  var heat = L.heatLayer(heatArray, {
    radius: 30,
    blur: 25,
    maxOpacity: .8,
    gradient: {0.3: 'yellow', 0.5: 'red'}
  }).addTo(myMap);

});

