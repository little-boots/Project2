    
function buildSankey(graph) {

    // define some margins before creating the svg object
    var margin = {top: 30, right: 10, bottom: 10, left: 10},
    width = 1200 - margin.left - margin.right,
    height = 800 - margin.top - margin.bottom;

    // clear the old graph
    d3.select("#diagram").html("")

    // add svg object to the diagram div
    var svg = d3.select("#diagram")
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // pick a d3 color scheme
    var color = d3.scaleOrdinal(d3.schemeCategory20c);

    // set some sankey diagram properties
    // nodewidth is how fat the node rectangles are
    // nodepadding sets the vertical padding between the nodes
    // requiring more padding between nodes also scales down the node height, creating a more "open" diagram with less overlapping
    var sankey = d3.sankey()
        .nodeWidth(30)
        .nodePadding(50) //
        .size([width, height]);
        
    // the sankey generator
    sankey
        .nodes(graph.nodes)
        .links(graph.links)
        .layout(1);

    // set up the nodes
    var node = svg.append("g").selectAll(".node").data(graph.nodes).enter().append("g")
        .attr("class", "node")
        .attr("transform", d => "translate(" + d.x + "," + d.y + ")")
        .call(d3.drag()
        .subject(d => d)
        .on("start", function() {this.parentNode.appendChild(this)})
        .on("drag", movenode));
    
    // add node rectangles and some hover text
    node.append("rect")
        .attr("height", d => d.dy)
        .attr("width", sankey.nodeWidth())
        .style("fill", d => d.color = color(d.name.replace(/ .*/, "")))
        .style("stroke", d => d3.rgb(d.color).darker(2))
        .append("title")
        .text(d => d.name + "\n" + "total = " + d.value);

    // add node labels
    node.append("text")
        .attr("x", -6)
        .attr("y", d => d.dy/2)
        .attr("dy", ".35em")
        .attr("text-anchor", "end")
        .attr("transform", null)
        .text(d => d.name)
        .filter(d => d.x < width / 2)
        .attr("x", 6 + sankey.nodeWidth())
        .attr("text-anchor", "start");

    // set up the links, which are the proportionate paths from node to node
    var link = svg.append("g").selectAll(".link").data(graph.links).enter().append("path")
        .attr("class", "link")
        .attr("d", sankey.link() )
        .style("stroke-width", d => Math.max(1, d.dy))
        .sort((a, b) => b.dy - a.dy); //sord order based on link width

    // function to allow dragging to improve readability
    function movenode(d) {
        d3.select(this).attr("transform", "translate(" + (d.x = Math.max(0, Math.min(width - d.dx, d3.event.x))) 
            + "," + (d.y = Math.max(0, Math.min(height - d.dy, d3.event.y))) + ")");
        sankey.relayout();
        link.attr("d", sankey.link() );
    }    
};


function init() {
    // get a reference to the dropdown select element
    var selector = d3.select("#selState");

    // clear the dropdown
    selector.html("");

    // populate state dropdown w list of states
    d3.json("/states", function(states) {
        states.forEach((state) => {
        selector
            .append("option")
            .text(state)
            .property("value", state);
        });
    
    // get the list of counties for the first (default) state
    getCounties(states[0])    
    });
}; // end of init()

function getCounties(state) {

    // get a reference to the dropdown element for counties
    var selector = d3.select("#selCounty");

    // clear the dropdown of any previous state's counties
    selector.html("")

    // populate counties dropdown w list of counties for the given state
    d3.json(`/counties/${state}`, function(counties) {
        counties.forEach((county) => {
            selector
                .append("option")
                .text(county)
                .property("value", county);
            
            // if the page has just loaded, draw a default sankey
            // set the flag so that only selected sankeys are drawn from this point until the page is reloaded
            if (first_time === true) {
                first_time = false;

                //draw the default sankey
                d3.json(`/sankeyData/${state}/${county}`, function(graph) {
                    buildSankey(graph)                  
                    });
            };           
        });
    });    

}; // end of getCounties()

// this is called whenever a new county is selected. time to draw a new sankey
function getBoth() {
    var state = d3.select("#selState").node().value
    var county = d3.select("#selCounty").node().value

    // get the sankey data for the selected state and county
    d3.json(`/sankeyData/${state}/${county}`, function(graph) {

    // build the graph for the selected state and county
    buildSankey(graph)
    });
}; // end of getBoth()

// set the flag to draw a default sankey if the page has just loaded
var first_time = true

// run this every time the page loads
init();



