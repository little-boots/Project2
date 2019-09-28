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
d3.json('/heatlist', function(response) {  

  for (var i = 0; i < response.len; i++) {  
      var label = "County: " + response['state_county'][i] + "<br>" + 
                  "Per Capita Usage: " + response['per_capita_usage'][i] + "<br>" +
                  "Addiction Index: " + response['addiction_index'][i];    
     L.marker([response['lat'][i], response['lon'][i]]).addTo(myMap)
      .bindPopup(label);
  }

  // Create a new marker cluster group
  // var markers = L.markerClusterGroup();

  // for (var i = 0; i < response.len; i++) {
  //     var label = "County: " + response['state_county'][i] + "<br>" + 
  //                 "Per Capita Usage: " + response['per_capita_usage'][i] + "<br>" +
  //                 "Addiction Index: " + response['addiction_index'][i];
  //     // Add a new marker to the cluster group and bind a pop-up
  //     markers.addLayer(L.marker([response['lat'][i], response['lon'][i]])
  //     .bindPopup(label));
  // }

  // Add our marker cluster layer to the map
  // myMap.addLayer(markers);

});

