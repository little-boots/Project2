
function buildChart(state,county) {
    // get the county data
    d3.json(`/data/${state}/${county}`).then((data) => {
        
        // the color palette
        var colors = ['#6c757d','#007bff','#28a745'];

        // create the chart object using the DOM element
        var chLine = document.getElementById("chLine");

        // build the chart data object for the line chart
        var chartData = {

            // x-axis labels  
            labels: data.years,
            
            // the series to plot
            datasets: [
                // first series
                {
                label: 'ARC States',
                data: data.ppc_nation,
                backgroundColor: 'transparent',
                borderColor: colors[0],
                borderWidth: 4,
                pointBackgroundColor: colors[0]
                },

                // second series
                {
                label: 'State',
                data: data.ppc_state,
                backgroundColor: 'transparent',
                borderColor: colors[1],
                borderWidth: 4,
                pointBackgroundColor: colors[1]
                },

                // third series
                {
                label: 'County',
                data: data.ppc_county,
                backgroundColor: 'transparent',
                borderColor: colors[2],
                borderWidth: 4,
                pointBackgroundColor: colors[2]
                }] 
        }; // end chartData

        // build the chart options object
        var options = {
            scales: {
                yAxes:[
                    {scaleLabel: 
                        {
                            display: true,
                            labelString: 'Pills Per Capita'
                        }
                    }
                ],
                xAxes:[
                    {scaleLabel: 
                        {
                            display: true,
                            labelString: 'Years'
                        }
                    }
                ]

                },
            legend: {
                display: true
            }
        }; // end options

        // Draw the chart
        // TODO: read the documentation and figure out how to destroy the "old" charts before drawing a new one. 
        // Does it only work with an older version of D3?
        if (chLine) {
            new Chart(chLine, {type: 'line', data: chartData, options: options});
        };         
    });
} // end BuildChart


function init() {
    // get a reference to the state dropdown select element
    var selector = d3.select("#selState");

    // clear the dropdown
    selector.html("");

    // populate state drop down w list of states
    d3.json("/states").then((states) => {
        states.forEach((state) => {
        selector
            .append("option")
            .text(state)
            .property("value", state);
        });
    
    // call the getCounties functon to get a default county for the first state in the list
    getCounties(states[0])
    
    });
}; // end of init()

function getCounties(state) {

    // get a refererence to the county dropdown select element
    var selector = d3.select("#selCounty");

    // clear the dropdown from a previous state's counties
    selector.html("")

    // populate counties dropdown w list of counties for the given state
    d3.json(`/counties/${state}`).then((counties) => {
        counties.forEach((county) => {
        selector
            .append("option")
            .text(county)
            .property("value", county);

            // if the page has just loaded, draw a default chart
            // and set the flag to false so that only selected charts are drawn from this point on
            if (first_time === true) {
                first_time = false;    
                buildChart(state,county)            
                  
            };
        });
    });
    
}; // end of getCounties()

function getBoth() {

    // this is called when a new county is selected. time to redraw the line chart for the new county
    var state = d3.select("#selState").node().value
    var county = d3.select("#selCounty").node().value

    buildChart(state,county);

}; // end of getBoth()

// create a flag to help draw a default line chart whenever the page is loaded.
var first_time = true

// run this once every time the page loads
init();

