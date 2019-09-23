function buildMetadata(sample) {

  // use d3 to select the panel with id of `#sample-metadata`
  var panel = d3.select('#sample-metadata');

  // use `.html('')` to clear any existing metadata
  panel.html('');

  // use `Object.entries` to add each key and value pair to the panel
  d3.json(`/metadata/${sample}`).then((sampleMetadata) => {
    Object.entries(sampleMetadata).forEach(([key, value]) =>
      panel
        .append('p')
        .html(`${key}: ${value}`)
    );

    // build the gauge chart
    var data = [{
      domain: {
        x: [0, 1],
        y: [0, 1]
      },
      value: sampleMetadata.WFREQ,
      type: 'indicator',
      mode: 'gauge+number',
      gauge: {
        axis: {range: [null, 9]},
        steps: [
          { range: [0, 1], color: 'rgb(248, 243, 236)' },
          { range: [1, 2], color: 'rgb(244, 241, 229)' },
          { range: [2, 3], color: 'rgb(233, 230, 202}' },
          { range: [3, 4], color: 'rgb(229, 231, 179)' },
          { range: [4, 5], color: 'rgb(213, 228, 157)' },
          { range: [5, 6], color: 'rgb(183, 204, 146)' },
          { range: [6, 7], color: 'rgb(140, 191, 136)' },
          { range: [7, 8], color: 'rgb(138, 187, 143)' },
          { range: [8, 9], color: 'rgb(133, 180, 138)' }
        ]
      }
    }];
    var layout = {
      title: '<b>Belly Button Washing Frequency</b><br>' +
            'Scrubs per Week'
    };
    Plotly.newPlot("gauge", data, layout)
  });

}

function buildCharts(sample) {

  // use `d3.json` to fetch the sample data for the plots
  d3.json(`/samples/${sample}`).then((sample) => {

    // build a pie chart using the sample data
    var data = [{
      values: sample.sample_values.slice(0, 10),
      labels: sample.otu_ids.slice(0, 10),
      type: 'pie',
      hovertext: sample.otu_labels.slice(0, 10),
      hovertemplate:
        'OTU ID: %{label}<br>' +
        'OTU Label: %{text}<br>' +
        'Sample Value: %{value}'
    }];
    Plotly.newPlot('pie', data);

    // build a bubble chart using the sample data
    var data =[{
      x: sample.otu_ids,
      y: sample.sample_values,
      mode: 'markers',
      marker: {
        size: sample.sample_values,
        color: sample.otu_ids,
        colorscale: 'Earth'
      },
      text: sample.otu_labels,
      hovertemplate:
        '(%{x}, %{y})<br>' +
        '%{text}'
    }];
    Plotly.newPlot('bubble', data);
  });

}

function init() {
  // grab a reference to the dropdown select element
  var selector = d3.select('#selDataset');

  // use the list of sample names to populate the select options
  d3.json('/names').then((sampleNames) => {
    sampleNames.forEach((sample) => {
      selector
        .append('option')
        .text(sample)
        .property('value', sample);
    });

    // use the first sample from the list to build the initial plots
    const firstSample = sampleNames[0];
    buildCharts(firstSample);
    buildMetadata(firstSample);
  });
}

function optionChanged(newSample) {
  // fetch new data each time a new sample is selected
  buildCharts(newSample);
  buildMetadata(newSample);
}

// initialize the dashboard
init();
